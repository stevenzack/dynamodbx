package dynamodbx

import (
	"errors"

	"github.com/iancoleman/strcase"
)

var (
	ErrItemNotFound         = errors.New("Item not found")
	ErrConditionalCheckFail = errors.New("ConditionalCheckFailedException: The conditional request failed")
)

func ToTableName(s string) string {
	return strcase.ToLowerCamel(s)
}
