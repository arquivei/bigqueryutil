package main

import (
	"fmt"
	"time"

<<<<<<< HEAD
	"github.com/arquivei/foundationkit/ref"
=======
>>>>>>> 782b339c4604ada956bae913b3aaaa02fa14b36f
	"github.com/bigqueryutil"
)

func main() {
<<<<<<< HEAD

	// QueryBuilderSpec represents the spec for the query builder
	var queryBuilderExample = bigqueryutil.QueryBuilderSpec{
		RepeatedColumns: map[string]struct{}{
			"AccessKey": {},
			"Owner":     {},
		},
		SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
			"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
			"FROM %s WHERE %s) WHERE r = 1;",
	}

	// TimeRange represents a time with a beginning and an end.
=======
	var queryBuilderExample = bigqueryutil.QueryBuilderSpec{
		RepeatedColumns: map[string]struct{}{
			"name": {},
			"age":  {},
			"sex":  {},
		},
	}

>>>>>>> 782b339c4604ada956bae913b3aaaa02fa14b36f
	var TimeRangeExample = bigqueryutil.TimeRange{
		From: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC),
	}

<<<<<<< HEAD
	// filter struct represents a filter for a query with Big Query's tags.
=======
>>>>>>> 782b339c4604ada956bae913b3aaaa02fa14b36f
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
<<<<<<< HEAD
		IsTaker:   ref.Bool(false),
	}

	// The projection fields are a required field on the HTTP API.
	var projectionExample = []string{"AccessKey"}

	// BuildColumnsClause returns the string that represents the columnsClauseBuilder with all columns required
	// by projection, or "*" if projection is nil.
	returnedColumns := bigqueryutil.BuildColumnsClause(queryBuilderExample, projectionExample)

	// EncodeBigqueryWhereClause transforms a struct into a bigquery's query and parameters list.
=======
		IsTaker:   ref(true),
	}

	var projectionExample []string

	returnedColumns := bigqueryutil.BuildColumnsClause(queryBuilderExample, projectionExample)
	fmt.Println(returnedColumns)

>>>>>>> 782b339c4604ada956bae913b3aaaa02fa14b36f
	whereExample, queryParametersExample, err := bigqueryutil.EncodeBigqueryWhereClause(filterExample)
	if err != nil {
		panic(err)
	}
<<<<<<< HEAD
	// These parameters will be passed to Big Query and will be used in the query.
	fmt.Printf("Big Query Parameters: \n%+v\n\n", queryParametersExample)

	// The query is the string that will be used in the BigQuery.
	sqlQuery := fmt.Sprintf(queryBuilderExample.SQLQuery, returnedColumns, "`TABLE_EXAMPLE`", whereExample)
	fmt.Printf("Sql Query: \n%+v\n", sqlQuery)

	//Output:
	/*
	   Big Query Parameters:
	   [{Name:Namespace Value:namespace} {Name:CreatedAtFrom Value:2022-01-01T00:00:00Z} {Name:CreatedAtTo Value:2022-02-01T00:00:00Z} {Name:Owner0 Value:owner1} {Name:Owner1 Value:owner2}]

	   Sql Query:
	   SELECT * EXCEPT(r) FROM (SELECT AccessKey, ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r FROM `TABLE_EXAMPLE` WHERE Namespace = @Namespace AND CreatedAt BETWEEN @CreatedAtFrom AND @CreatedAtTo AND Owner IN (@Owner0,@Owner1) AND NOT IsTaker) WHERE r = 1;
	*/

=======

	fmt.Printf("%+v\n", whereExample)
	fmt.Printf("%+v\n", queryParametersExample)

}

func ref[T any](v T) *T {
	return &v
>>>>>>> 782b339c4604ada956bae913b3aaaa02fa14b36f
}
