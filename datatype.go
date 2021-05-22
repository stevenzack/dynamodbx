package dynamodbx

import (
	"errors"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func Time(t time.Time) *dynamodb.AttributeValue {
	av, _ := dynamodbattribute.Marshal(t)
	return av
}

func ToDynamoDBType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64, reflect.Float32, reflect.Float64:
		return "N", nil
	case reflect.String:
		return "S", nil
	case reflect.Bool:
		return "B", nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			// []byte
			return "B", nil
		case reflect.String, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64, reflect.Float32, reflect.Float64:
			return "L", nil
		}
	case reflect.Struct:
		switch t.String() {
		case "time.Time":
			return "S", nil
		}
	}
	return "", errors.New("unsupport field type:" + t.String() + ",kind=" + t.Kind().String())
}
