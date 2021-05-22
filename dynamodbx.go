package dynamodbx

import (
	"errors"

	"github.com/iancoleman/strcase"
)

var(
	ErrItemNotFound=errors.New("Item not found")
)

func ToTableName(s string) string {
	return strcase.ToLowerCamel(s)
}
