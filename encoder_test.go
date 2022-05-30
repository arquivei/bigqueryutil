package bigqueryutil

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/arquivei/foundationkit/ref"
	"github.com/stretchr/testify/assert"
)

func TestMarshalWhereClause(t *testing.T) {
	type want struct {
		query  string
		params []bigquery.QueryParameter
	}

	testCases := []struct {
		name    string
		arg     interface{}
		want    want
		wantErr bool
	}{
		{
			name: "string",
			arg: struct {
				Namespace string
			}{
				Namespace: "tiramissu",
			},
			want: want{
				query: "Namespace = @Namespace",
				params: []bigquery.QueryParameter{
					{Name: "Namespace", Value: "tiramissu"},
				},
			},
		},
		{
			name: "string,omitempty",
			arg: struct {
				Namespace *string
				Empty     string `bq:",omitempty"`
			}{
				Namespace: ref.Str("tiramissu"),
			},
			want: want{
				query: "Namespace = @Namespace",
				params: []bigquery.QueryParameter{
					{Name: "Namespace", Value: "tiramissu"},
				},
			},
		},
		{
			name: "and",
			arg: struct {
				Field1 string
				Field2 string
				Field3 string
			}{
				Field1: "Field1",
				Field2: "Field2",
				Field3: "Field3",
			},
			want: want{
				query: "Field1 = @Field1 AND Field2 = @Field2 AND Field3 = @Field3",
				params: []bigquery.QueryParameter{
					{Name: "Field1", Value: "Field1"},
					{Name: "Field2", Value: "Field2"},
					{Name: "Field3", Value: "Field3"},
				},
			},
		},
		{
			name: "slice",
			arg: struct {
				Owners []string `bq:"Owner"`
			}{
				Owners: []string{"19427033000140", "03160081000185"},
			},
			want: want{
				query: "Owner IN (@Owner0,@Owner1)",
				params: []bigquery.QueryParameter{
					{Name: "Owner0", Value: "19427033000140"},
					{Name: "Owner1", Value: "03160081000185"},
				},
			},
		},
		{
			name: "slice,unnest",
			arg: struct {
				OwnerRoles []string `bq:",unnest"`
			}{
				OwnerRoles: []string{"role1", "role2"},
			},
			want: want{
				query: "EXISTS (SELECT * FROM UNNEST(OwnerRoles) AS x WHERE x IN (@OwnerRoles0,@OwnerRoles1))",
				params: []bigquery.QueryParameter{
					{Name: "OwnerRoles0", Value: "role1"},
					{Name: "OwnerRoles1", Value: "role2"},
				},
			},
		},
		{
			name: "string,unnest",
			arg: struct {
				OwnerRoles string `bq:",unnest"`
			}{
				OwnerRoles: "role",
			},
			want: want{
				query: "EXISTS (SELECT * FROM UNNEST(OwnerRoles) AS x WHERE x = @OwnerRoles)",
				params: []bigquery.QueryParameter{
					{Name: "OwnerRoles", Value: "role"},
				},
			},
		},
		{
			name: "timerange",
			arg: struct {
				EmissionDate TimeRange
			}{
				EmissionDate: TimeRange{
					From: time.Date(2020, 01, 03, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 04, 0, 0, 0, 0, time.UTC),
				},
			},
			want: want{
				query: "EmissionDate BETWEEN @EmissionDateFrom AND @EmissionDateTo",
				params: []bigquery.QueryParameter{
					{Name: "EmissionDateFrom", Value: "2020-01-03T00:00:00Z"},
					{Name: "EmissionDateTo", Value: "2020-01-04T00:00:00Z"},
				},
			},
		},
		{
			name: "bool,true",
			arg: struct {
				IsTaker bool
			}{
				IsTaker: true,
			},
			want: want{
				query:  "IsTaker",
				params: []bigquery.QueryParameter{},
			},
		},
		{
			name: "bool,false",
			arg: struct {
				IsTaker bool
			}{
				IsTaker: false,
			},
			want: want{
				query:  "NOT IsTaker",
				params: []bigquery.QueryParameter{},
			},
		},
		{
			name: "full_example",
			arg: struct {
				Namespace               string     `bq:",omitempty"`
				CreatedAt               *TimeRange `bq:",omitempty"`
				EmissionDate            *TimeRange `bq:",omitempty"`
				EmissionDateWithoutTime *TimeRange `bq:",omitempty" format:"2006-01-02"`
				Owners                  []string   `bq:"Owner,omitempty"`
				OwnerRoles              []string   `bq:",unnest,omitempty"`
				IsTaker                 *bool      `bq:",omitempty"`
			}{
				Namespace: "tiramissu",
				CreatedAt: &TimeRange{
					From: time.Date(2020, 01, 01, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 02, 0, 0, 0, 0, time.UTC),
				},
				EmissionDate: &TimeRange{
					From: time.Date(2020, 01, 03, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 04, 0, 0, 0, 0, time.UTC),
				},
				EmissionDateWithoutTime: &TimeRange{
					From: time.Date(2020, 01, 05, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 06, 0, 0, 0, 0, time.UTC),
				},
				Owners:     []string{"owner1", "owner2"},
				OwnerRoles: []string{"role1", "role2"},
				IsTaker:    ref.Bool(false),
			},
			want: want{
				query: `Namespace = @Namespace` +
					` AND CreatedAt BETWEEN @CreatedAtFrom AND @CreatedAtTo` +
					` AND EmissionDate BETWEEN @EmissionDateFrom AND @EmissionDateTo` +
					` AND EmissionDateWithoutTime BETWEEN @EmissionDateWithoutTimeFrom AND @EmissionDateWithoutTimeTo` +
					` AND Owner IN (@Owner0,@Owner1)` +
					` AND EXISTS (SELECT * FROM UNNEST(OwnerRoles) AS x WHERE x IN (@OwnerRoles0,@OwnerRoles1))` +
					` AND NOT IsTaker`,
				params: []bigquery.QueryParameter{
					{Name: "Namespace", Value: "tiramissu"},
					{Name: "CreatedAtFrom", Value: "2020-01-01T00:00:00Z"},
					{Name: "CreatedAtTo", Value: "2020-01-02T00:00:00Z"},
					{Name: "EmissionDateFrom", Value: "2020-01-03T00:00:00Z"},
					{Name: "EmissionDateTo", Value: "2020-01-04T00:00:00Z"},
					{Name: "EmissionDateWithoutTimeFrom", Value: "2020-01-05"},
					{Name: "EmissionDateWithoutTimeTo", Value: "2020-01-06"},
					{Name: "Owner0", Value: "owner1"},
					{Name: "Owner1", Value: "owner2"},
					{Name: "OwnerRoles0", Value: "role1"},
					{Name: "OwnerRoles1", Value: "role2"},
				},
			},
		},
		{
			name: "invalid_type",
			arg: struct {
				Invalid uintptr
			}{},
			wantErr: true,
		},
		{
			name: "invalid_struct",
			arg: struct {
				Invalid struct{}
			}{},
			wantErr: true,
		},
		{
			name:    "filter is not a struct",
			arg:     "bla",
			wantErr: true,
		},
		{
			name: "empty slice",
			arg: struct {
				Namespace               string     `bq:",omitempty"`
				CreatedAt               *TimeRange `bq:",omitempty"`
				EmissionDate            *TimeRange `bq:",omitempty"`
				EmissionDateWithoutTime *TimeRange `bq:",omitempty" format:"2006-01-02"`
				Owners                  []string   `bq:"Owner,omitempty"`
				OwnerRoles              []string   `bq:",unnest,omitempty"`
				IsTaker                 *bool      `bq:",omitempty"`
			}{
				Namespace: "tiramissu",
				CreatedAt: &TimeRange{
					From: time.Date(2020, 01, 01, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 02, 0, 0, 0, 0, time.UTC),
				},
				EmissionDate: &TimeRange{
					From: time.Date(2020, 01, 03, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 04, 0, 0, 0, 0, time.UTC),
				},
				EmissionDateWithoutTime: &TimeRange{
					From: time.Date(2020, 01, 05, 0, 0, 0, 0, time.UTC),
					To:   time.Date(2020, 01, 06, 0, 0, 0, 0, time.UTC),
				},
				Owners:     []string{},
				OwnerRoles: []string{"role1", "role2"},
				IsTaker:    ref.Bool(false),
			},
			want: want{
				query: `Namespace = @Namespace` +
					` AND CreatedAt BETWEEN @CreatedAtFrom AND @CreatedAtTo` +
					` AND EmissionDate BETWEEN @EmissionDateFrom AND @EmissionDateTo` +
					` AND EmissionDateWithoutTime BETWEEN @EmissionDateWithoutTimeFrom AND @EmissionDateWithoutTimeTo` +
					` AND EXISTS (SELECT * FROM UNNEST(OwnerRoles) AS x WHERE x IN (@OwnerRoles0,@OwnerRoles1))` +
					` AND NOT IsTaker`,
				params: []bigquery.QueryParameter{
					{Name: "Namespace", Value: "tiramissu"},
					{Name: "CreatedAtFrom", Value: "2020-01-01T00:00:00Z"},
					{Name: "CreatedAtTo", Value: "2020-01-02T00:00:00Z"},
					{Name: "EmissionDateFrom", Value: "2020-01-03T00:00:00Z"},
					{Name: "EmissionDateTo", Value: "2020-01-04T00:00:00Z"},
					{Name: "EmissionDateWithoutTimeFrom", Value: "2020-01-05"},
					{Name: "EmissionDateWithoutTimeTo", Value: "2020-01-06"},
					{Name: "OwnerRoles0", Value: "role1"},
					{Name: "OwnerRoles1", Value: "role2"},
				},
			},
		},
	}
	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, p, err := EncodeBigqueryWhereClause(tt.arg)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want.query, q)
			assert.Equal(t, tt.want.params, p)
		})
	}
}
