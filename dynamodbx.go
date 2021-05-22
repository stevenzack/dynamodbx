package dynamodbx

import "github.com/iancoleman/strcase"

func ToTableName(s string) string {
	return strcase.ToLowerCamel(s)
}
