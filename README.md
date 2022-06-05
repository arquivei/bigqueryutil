# BigQueryUtil

### A Golang library for building an SQL query based on a filter represented by a given structure.

---------------------

## Table of Contents

  - [1. Description](#Description)
  - [2. Technology Stack](#TechnologyStack)
  - [3. Getting Started](#GettingStarted)
  - [4. Changelog](#Changelog)
  - [5. Collaborators](#Collaborators)
  - [6. Contributing](#Contributing)
  - [7. Versioning](#Versioning)
  - [8. License](#License)
  - [9. Contact Information](#ContactInformation)

## <a name="Description" /> 1. Description

BigQueryUtil is a library to build a SQL query and Big Query Parameters based on provided columns, a sql template and a filter.

## <a name="TechnologyStack" /> 2. Technology Stack

| **Stack**     | **Version** |
|---------------|-------------|
| Golang        | v1.18       |
| golangci-lint | v1.45       |

## <a name="GettingStarted" /> 3. Getting Started

- ### <a name="Prerequisites" /> Prerequisites

  - Any [Golang](https://go.dev/doc/install) programming language version installed, preferred 1.18 or later.

- ### <a name="Install" /> Install
  
  ```
  go get -u github.com/arquivei/bigqueryutil
  ```

- ### <a name="ConfigurationSetup" /> Configuration Setup

  ```
  go mod vendor
  go mod tidy
  ```

- ### <a name="Usage" /> Usage
  
  - Import the package

    ```go
    import (
        "github.com/arquivei/bigqueryutil"
    )
    ```
  
  - Instantiate the QueryBuilderSpec struct that represents the spec for the query builder
    ```
    var queryBuilderExample = bigqueryutil.QueryBuilderSpec{
      RepeatedColumns: map[string]struct{}{
        "AccessKey": {},
        "Owner":     {},
      },
      SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
        "ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
        "FROM %s WHERE %s) WHERE r = 1;",
    }
    ```
  
  - Declare the TimeRange struct that represents a time with a beginning and an end.
    ```
    var TimeRangeExample = bigqueryutil.TimeRange{
      From: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
      To:   time.Date(2022, 2, 1, 0, 0, 0, 0, time.UTC),
    }
    ```
  
  - Declare and instantiante the struct that represents a filter for a query with Big Query's tags.
    ```
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
      IsTaker:   ref.Bool(false),
    }
    ```

  - Declare the projection fields that will be required field on the HTTP API.
    ```
    var projectionExample = []string{"AccessKey"}
    ```

  - Instantiate the BuildColumnsClause that will return the string that represents the columnsClauseBuilder with all columns required by projection, or "*" if projection is nil.
    ```
    returnedColumns := bigqueryutil.BuildColumnsClause(queryBuilderExample,projectionExample)
    ```

  - Instantiate the EncodeBigqueryWhereClause that transforms a struct into a bigquery's query and parameters list.
    ```
    whereExample, queryParametersExample, err := bigqueryutil.EncodeBigqueryWhereClause(filterExample)
    if err != nil {
      panic(err)
    }
    ```
  
  - These parameters will be passed to Big Query and will be used in the query.
    ```
    fmt.Printf("Big Query Parameters: \n%+v\n\n", queryParametersExample)
    ```

  - The query is the string that will be used in the BigQuery.
    ```
	sqlQuery := fmt.Sprintf(queryBuilderExample.SQLQuery, returnedColumns, "`TABLE_EXAMPLE`", whereExample)
	fmt.Printf("Sql Query: \n%+v\n", sqlQuery)
    ```

  - Output
    ```
	/*
	   Big Query Parameters:
	   [{Name:Namespace Value:namespace} {Name:CreatedAtFrom Value:2022-01-01T00:00:00Z} {Name:CreatedAtTo Value:2022-02-01T00:00:00Z} {Name:Owner0 Value:owner1} {Name:Owner1 Value:owner2}]

	   Sql Query:
	   SELECT * EXCEPT(r) FROM (SELECT AccessKey, ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r FROM `TABLE_EXAMPLE` WHERE Namespace = @Namespace AND CreatedAt BETWEEN @CreatedAtFrom AND @CreatedAtTo AND Owner IN (@Owner0,@Owner1) AND NOT IsTaker) WHERE r = 1;
	*/    
    ```

- ### <a name="Examples" /> Examples
  
  - [Sample usage](https://github.com/arquivei/bigqueryutil/blob/master/examples/main.go)

## <a name="Changelog" /> 4. Changelog

  - **bigqueryutil 0.1.0 (DATE)**
  
    - [New] Decoupling this package from Arquivei's API projects.
    - [New] Setting github's workflow with golangci-lint 
    - [New] Example for usage.
    - [New] Documents: Code of Conduct, Contributing, License and Readme.

## <a name="Collaborators" /> 5. Collaborators

- ### <a name="Authors" /> Authors
  
  <!-- markdownlint-disable -->
  <!-- prettier-ignore-start -->
	<table>
	<tr>
		<td align="center"><a href="https://github.com/victormn"><img src="https://avatars.githubusercontent.com/u/9757545?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Victor Nunes</b></sub></a></td>
		<td align="center"><a href="https://github.com/rjfonseca"><img src="https://avatars.githubusercontent.com/u/151265?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Rodrigo Fonseca</b></sub></a></td>
	</tr>
	</table>
  <!-- markdownlint-restore -->
  <!-- prettier-ignore-end -->

- ### <a name="Maintainers" /> Maintainers
  
  <!-- markdownlint-disable -->
  <!-- prettier-ignore-start -->
	<table>
	<tr>
		<td align="center"><a href="https://github.com/rilder-almeida"><img src="https://avatars.githubusercontent.com/u/49083200?v=4s=100" width="100px;" alt=""/><br /><sub><b>Rilder Almeida</b></sub></a></td>
	</tr>
	</table>
  <!-- markdownlint-restore -->
  <!-- prettier-ignore-end -->

## <a name="Contributing" /> 6. Contributing

  Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## <a name="Versioning" /> 7. Versioning

  We use [Semantic Versioning](http://semver.org/) for versioning. For the versions
  available, see the [tags on this repository](https://github.com/arquivei/bigqueryutil/tags).

## <a name="License" /> 8. License
  
This project is licensed under the BSD 3-Clause - see the [LICENSE.md](LICENSE.md) file for details.

## <a name="ContactInformation" /> 9. Contact Information

  Contacts can be made by email: [rilder.almeida@arquivei.com.br](mailto:rilder.almeida@arquivei.com.br)
