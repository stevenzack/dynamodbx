package dynamodbx

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/iancoleman/strcase"
)

var (
	ErrItemNotFound         = errors.New("Item not found")
	ErrConditionalCheckFail = errors.New("ConditionalCheckFailedException: The conditional request failed")
)

func ToTableName(s string) string {
	return strcase.ToLowerCamel(s)
}

func AddStringToSet(attributes map[string]*dynamodb.AttributeValue, k, v string) bool {
	av := attributes[k]
	if av == nil {
		av = &dynamodb.AttributeValue{}
	} else if len(av.L) > 0 {
		for _, item := range av.L {
			if item.S != nil {
				if *item.S == v {
					return false
				}
			}
		}
	}

	av.L = append(av.L, &dynamodb.AttributeValue{S: &v})
	attributes[k] = av
	return true
}
