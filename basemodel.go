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
	model, _, e := NewBaseModelWithCreated(region, data)
	return model, e
}
func NewBaseModelWithCreated(region string, data interface{}) (*BaseModel, bool, error) {
	t := reflect.TypeOf(data)
	b := &BaseModel{
		Type:      t,
		secondary: -1,
		TableName: t.Name(),
	}
	if t.NumField() == 0 {
		return nil, false, errors.New("Invalid struct [" + t.Name() + "] : no field in struct")
	}
	var e error
	b.session, e = session.NewSession(aws.NewConfig().WithRegion(region))
	if e != nil {
		return nil, false, e
	}
	b.Client = dynamodb.New(b.session)

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data必须是非指针类型")
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
				return nil, false, errors.New("The first field " + field.Name + "'s type must be one of uint,uint32,uint64,uint16,string")
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
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
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
			req := &dynamodb.CreateTableInput{
				TableName:   &b.TableName,
				BillingMode: aws.String("PAY_PER_REQUEST"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(b.dbTags[0]),
						KeyType:       aws.String("HASH"),
					},
				},
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String(b.dbTags[0]),
						AttributeType: aws.String(b.dnmTypes[0]),
					},
				},
			}

			//secondary
			if b.secondary > -1 {
				req.KeySchema = append(req.KeySchema, &dynamodb.KeySchemaElement{
					AttributeName: aws.String(b.dbTags[b.secondary]),
					KeyType:       aws.String("RANGE"),
				})
				req.AttributeDefinitions = append(req.AttributeDefinitions, &dynamodb.AttributeDefinition{
					AttributeName: aws.String(b.dbTags[b.secondary]),
					AttributeType: aws.String(b.dnmTypes[b.secondary]),
				})
			}

			_, e := b.Client.CreateTable(req)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}

			//create indexes
			idxReq := &dynamodb.UpdateTableInput{
				TableName: aws.String(b.TableName),
			}
			for _, key := range localIndexes {
				idxReq.GlobalSecondaryIndexUpdates = append(idxReq.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
					Create: &dynamodb.CreateGlobalSecondaryIndexAction{
						IndexName: aws.String(ToIndexName(key)),
						KeySchema: key,
						Projection: &dynamodb.Projection{
							ProjectionType: aws.String("ALL"),
						},
					},
				})
			}
			_, e = b.Client.UpdateTable(idxReq)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			return b, true, nil
		}
		log.Println(e)
		return nil, false, e
	}

	//index check
	m := make(map[string][]*dynamodb.KeySchemaElement)
	for _, v := range localIndexes {
		m[ToIndexName(v)] = v
	}
	localIndexes = m

	idxReq := &dynamodb.UpdateTableInput{
		TableName: aws.String(b.TableName),
	}
	remoteIndexes := make(map[string]struct{})
	//drop remote index
	for _, idx := range tableInfo.Table.GlobalSecondaryIndexes {
		remoteIndexes[*idx.IndexName] = struct{}{}
		if _, ok := localIndexes[*idx.IndexName]; !ok {
			log.Println("Remote index to be dropped: ", b.TableName+"."+*idx.IndexName)
			idxReq.GlobalSecondaryIndexUpdates = append(idxReq.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Delete: &dynamodb.DeleteGlobalSecondaryIndexAction{
					IndexName: idx.IndexName,
				},
			})
		}
	}

	//create index
	for k, v := range localIndexes {
		if _, ok := remoteIndexes[k]; !ok {
			log.Println("Remote index to be created: ", b.TableName, ".", k)
			idxReq.GlobalSecondaryIndexUpdates = append(idxReq.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(k),
					KeySchema: v,
					Projection: &dynamodb.Projection{
						ProjectionType: aws.String("ALL"),
					},
				},
			})
		}
	}

	//apply index changed
	if len(idxReq.GlobalSecondaryIndexUpdates) > 0 {
		_, e := b.Client.UpdateTable(idxReq)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
	}
	return b, false, nil
}

func (b *BaseModel) Insert(v interface{}) error {
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
