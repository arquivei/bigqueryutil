package bigqueryutil

// QueryBuilderSpec represents the spec for the query builder
type QueryBuilderSpec struct {
	RepeatedColumns map[string]struct{}
	SQLQuery        string
}
