package dynamodbx

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func ToIndexName(key []*dynamodb.KeySchemaElement) string {
	hash := ""
	secondary := ""
	for _, v := range key {
		if *v.KeyType == "HASH" {
			hash = *v.AttributeName
			continue
		}

		if *v.KeyType == "RANGE" {
			secondary = *v.AttributeName
			continue
		}
	}
	if secondary != "" {
		secondary = "-" + secondary
	}
	return hash + secondary + "-index"
}

func (b *BaseModel) GetIndexes() ([]*dynamodb.GlobalSecondaryIndexDescription, error) {
	res, e := b.Client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(b.TableName),
	})
	if e != nil {
		return nil, e
	}
	return res.Table.GlobalSecondaryIndexes, nil
}
