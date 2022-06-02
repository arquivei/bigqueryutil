package main

import (
	"fmt"
	"time"

	"github.com/bigqueryutil"
)

func main() {
	var queryBuilderExample = bigqueryutil.QueryBuilderSpec{
		RepeatedColumns: map[string]struct{}{
			"name": {},
			"age":  {},
			"sex":  {},
		},
	}

	var TimeRangeExample = bigqueryutil.TimeRange{
		From: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	type filter struct {
		Namespace string                  `bq:",omitempty"`
		CreatedAt *bigqueryutil.TimeRange `bq:",omitempty"`
		Owners    []string                `bq:"Owner,omitempty"`
		IsTaker   *bool                   `bq:",omitempty"`
	}

	var filterExample = filter{
		Namespace: "namespace",
		CreatedAt: &TimeRangeExample,
		Owners:    []string{"owner1", "owner2"},
		IsTaker:   ref(true),
	}

	var projectionExample []string

	returnedColumns := bigqueryutil.BuildColumnsClause(queryBuilderExample, projectionExample)
	fmt.Println(returnedColumns)

	whereExample, queryParametersExample, err := bigqueryutil.EncodeBigqueryWhereClause(filterExample)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", whereExample)
	fmt.Printf("%+v\n", queryParametersExample)

}

func ref[T any](v T) *T {
	return &v
}
