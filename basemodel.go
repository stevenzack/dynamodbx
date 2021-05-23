package dynamodbx

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type BaseModel struct {
	TableName string
	Type      reflect.Type
	session   *session.Session
	Client    *dynamodb.DynamoDB
	dbTags    []string
	secondary int
	dnmTypes  []string
}

const (
	TAG               = "dynamodbav"
	TAG_INDEX         = "index"
	TAG_SECONDARY     = "secondary"
	TAG_SECONDARY_FOR = "secondaryfor"
)

func NewBaseModel(region string, data interface{}) (*BaseModel, error) {
	t := reflect.TypeOf(data)
	b := &BaseModel{
		Type:      t,
		secondary: -1,
		TableName: ToTableName(t.Name()),
	}

	if t.NumField() == 0 {
		return nil, errors.New("Invalid struct [" + t.Name() + "] : no field in struct")
	}
	var e error
	b.session, e = session.NewSession(aws.NewConfig().WithRegion(region))
	if e != nil {
		return nil, e
	}
	b.Client = dynamodb.New(b.session)

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, errors.New("data必须是非指针类型")
	}

	localIndexes := make(map[string][]*dynamodb.KeySchemaElement)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if i == 0 {
			switch field.Type.Kind() {
			case reflect.Uint,
				reflect.Uint64,
				reflect.Uint32,
				reflect.Uint16,
				reflect.String:
			default:
				return nil, errors.New("The first field " + field.Name + "'s type must be one of uint,uint32,uint64,uint16,string")
			}
		}

		//dbTag
		dbTag, ok := field.Tag.Lookup("dynamodbav")
		if !ok {
			dbTag = field.Name
		} else {
			dbTag = strToolkit.SubBefore(dbTag, ",", dbTag)
		}

		//secondary
		if _, ok := field.Tag.Lookup(TAG_SECONDARY); ok {
			b.secondary = i
		}

		//dnmType
		dnmType, e := ToDynamoDBType(field.Type)
		if e != nil {
			log.Println(e)
			return nil, fmt.Errorf("Field %s:%w", field.Name, e)
		}
		//index
		if _, ok := field.Tag.Lookup(TAG_INDEX); ok {
			localIndexes[dbTag] = append(localIndexes[dbTag], &dynamodb.KeySchemaElement{
				AttributeName: aws.String(dbTag),
				KeyType:       aws.String("HASH"),
			})
		} else if target, ok := field.Tag.Lookup(TAG_SECONDARY_FOR); ok {
			localIndexes[target] = append(localIndexes[target], &dynamodb.KeySchemaElement{
				AttributeName: aws.String(dbTag),
				KeyType:       aws.String("RANGE"),
			})
		}

		b.dbTags = append(b.dbTags, dbTag)
		b.dnmTypes = append(b.dnmTypes, dnmType)
	}

	tableInfo, e := b.Client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(b.TableName),
	})
	if e != nil {
		if strings.HasPrefix(e.Error(), dynamodb.ErrCodeResourceNotFoundException) {
			//create table
			return nil, errors.New("Table " + b.TableName + " to be created")
		}
		log.Println(e)
		return nil, e
	}

	//index check
	m := make(map[string][]*dynamodb.KeySchemaElement)
	for _, v := range localIndexes {
		m[ToIndexName(v)] = v
	}
	localIndexes = m

	remoteIndexes := make(map[string]struct{})
	//drop remote index
	for _, idx := range tableInfo.Table.GlobalSecondaryIndexes {
		remoteIndexes[*idx.IndexName] = struct{}{}
		if _, ok := localIndexes[*idx.IndexName]; !ok {
			return nil, errors.New("Remote index to be dropped: " + b.TableName + "." + *idx.IndexName)
		}
	}

	//create index
	for k := range localIndexes {
		if _, ok := remoteIndexes[k]; !ok {
			return nil, errors.New("Remote index to be created: " + b.TableName + "." + k)
		}
	}

	return b, nil
}

func (b *BaseModel) Put(v interface{}) error {
	av, e := dynamodbattribute.MarshalMap(v)
	if e != nil {
		return e
	}
	_, e = b.Client.PutItem(&dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.TableName),
	})
	return e
}

func (b *BaseModel) Insert(v interface{}) error {
	av, e := dynamodbattribute.MarshalMap(v)
	if e != nil {
		return e
	}
	_, e = b.Client.PutItem(&dynamodb.PutItemInput{
		TableName:           &b.TableName,
		Item:                av,
		ConditionExpression: aws.String(`attribute_not_exists(` + b.dbTags[0] + `)`),
	})
	return e
}

func (b *BaseModel) Get(id interface{}, secondary ...interface{}) (interface{}, error) {
	key := make(map[string]*dynamodb.AttributeValue)
	var e error
	key[b.dbTags[0]], e = dynamodbattribute.Marshal(id)
	if e != nil {
		return nil, e
	}
	if len(secondary) > 0 {
		key[b.dbTags[b.secondary]], e = dynamodbattribute.Marshal(secondary[0])
		if e != nil {
			return nil, e
		}
	}
	res, e := b.Client.GetItem(&dynamodb.GetItemInput{
		TableName: &b.TableName,
		Key:       key,
	})
	if e != nil {
		return nil, e
	}

	if len(res.Item) == 0 {
		return nil, ErrItemNotFound
	}

	v := reflect.New(b.Type)
	e = dynamodbattribute.UnmarshalMap(res.Item, v.Interface())
	if e != nil {
		return nil, e
	}
	return v.Interface(), nil
}

func (b *BaseModel) FindWhere(key string, value interface{}) (interface{}, error) {
	av, e := dynamodbattribute.Marshal(value)
	if e != nil {
		return nil, e
	}

	res, e := b.Client.Query(&dynamodb.QueryInput{
		TableName:              &b.TableName,
		KeyConditionExpression: aws.String("#" + key + "=:" + key),
		ExpressionAttributeNames: map[string]*string{
			"#" + key: &key,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":" + key: av,
		},
		Limit: aws.Int64(1),
	})
	if e != nil {
		return nil, e
	}
	if len(res.Items) == 0 {
		return nil, ErrItemNotFound
	}
	v := reflect.New(b.Type)
	e = dynamodbattribute.UnmarshalMap(res.Items[0], v.Interface())
	if e != nil {
		return nil, e
	}
	return v.Interface(), nil
}

func (b *BaseModel) Update(key map[string]*dynamodb.AttributeValue, updator string, args map[string]*dynamodb.AttributeValue) (int64, error) {
	_, e := b.Client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 &b.TableName,
		Key:                       key,
		UpdateExpression:          &updator,
		ExpressionAttributeValues: args,
		ConditionExpression:       aws.String(`attribute_exists(` + b.dbTags[0] + `)`),
	})
	if e != nil {
		if e.Error() == ErrConditionalCheckFail.Error() {
			return 0, nil
		}
		return 0, e
	}
	return 1, nil
}

func (b *BaseModel) UpdateWhere(input *dynamodb.UpdateItemInput) (int64, error) {
	input.TableName = &b.TableName
	input.ConditionExpression = aws.String(`attribute_exists(` + b.dbTags[0] + `)`)
	_, e := b.Client.UpdateItem(input)
	if e != nil {
		if e.Error() == ErrConditionalCheckFail.Error() {
			return 0, nil
		}
		return 0, e
	}
	return 1, nil
}
