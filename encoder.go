package bigqueryutil

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/arquivei/foundationkit/errors"
)

// EncodeBigqueryWhereClause transforms a struct into a bigquery's query and parameters list.
//
// 	struct {
// 		Namespace               string                 	`bq:",omitempty"`
// 		CreatedAt               *TimeRange 			   	`bq:",omitempty"`
// 		EmissionDate            *TimeRange 			   	`bq:",omitempty"`
// 		EmissionDateWithoutTime *TimeRange 			   	`bq:",omitempty" format:"2006-01-02"`
// 		Owners                  []string               	`bq:"Owner,omitempty"`
// 		OwnerRoles              []string               	`bq:",unnest,omitempty"`
// 		IsTaker                 *bool                  	`bq:",omitempty"`
// 	}
// nolint: gocognit,cyclop
func EncodeBigqueryWhereClause(filter interface{}) (string, []bigquery.QueryParameter, error) {
	rv := reflect.ValueOf(filter)
	if rv.Kind() != reflect.Struct {
		return "", nil, errors.New("filter must be a struct: " + rv.Kind().String())
	}

	sb := strings.Builder{}  // main string builder
	fsb := strings.Builder{} // string builder for a single field

	nFields := rv.Type().NumField()

	// This is an approximation. In reality, TimeRange uses two slots and booleans use none.
	params := make([]bigquery.QueryParameter, 0, nFields)

	for i := 0; i < nFields; i++ {
		// Get value, type and tags
		fvalue := rv.Field(i)
		ftype := rv.Type().Field(i)
		fparam := parseFieldParameters(ftype.Tag.Get("bq"))

		// Skip zero values if omitempty is enabled for the field
		if fparam.omitEmpty && fvalue.IsZero() {
			continue
		}

		// Rename field if specified a new name inside the tag
		name := ftype.Name
		if fparam.name != "" {
			name = fparam.name
		}

		// Fix kind and value if field is a pointer
		fkind := ftype.Type.Kind()
		if fkind == reflect.Ptr {
			fvalue = fvalue.Elem()
			fkind = fvalue.Kind()
		}

		fsb.Reset()

		// the filed will be temporary stored
		if fparam.unnest {
			fsb.WriteString("EXISTS (SELECT * FROM UNNEST(")
			fsb.WriteString(name)
			fsb.WriteString(") AS x WHERE x")
		} else {
			fsb.WriteString(name)
		}

		switch fkind {
		case reflect.String:
			fsb.WriteString(" = @")
			fsb.WriteString(name)
			params = AppendParam(params, name, fvalue.Interface())
		case reflect.Slice:
			if rv.Field(i).Len() == 0 {
				continue
			}
			fsb.WriteString(" IN (")
			for j := 0; j < fvalue.Len(); j++ {
				elemName := name + strconv.Itoa(j)
				if j > 0 {
					fsb.WriteString(",")
				}
				fsb.WriteString("@")
				fsb.WriteString(elemName)
				params = AppendParam(params, elemName, fvalue.Index(j).Interface())
			}
			fsb.WriteString(")")
		case reflect.Bool:
			if !fvalue.Bool() {
				fsb.Reset()
				fsb.WriteString("NOT ")
				fsb.WriteString(name)
			}
		case reflect.Struct:
			switch v := fvalue.Interface().(type) {
			case TimeRange:
				fsb.WriteString(" BETWEEN @")
				fsb.WriteString(name)
				fsb.WriteString("From AND @")
				fsb.WriteString(name)
				fsb.WriteString("To")
				format := ftype.Tag.Get("format")
				if format == "" {
					format = time.RFC3339
				}
				params = AppendParam(params, name+"From", v.From.Format(format))
				params = AppendParam(params, name+"To", v.To.Format(format))
			default:
				return "", nil, errors.New(name + " struct is not supported")
			}
		default:
			return "", nil, errors.New(name + " is of unknown type: " + fkind.String())
		}
		if fparam.unnest {
			fsb.WriteString(")")
		}

		// Apped to main query
		if sb.Len() > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(fsb.String())
	}
	return sb.String(), params, nil
}

func parseFieldParameters(tag string) fieldParameters {
	var params fieldParameters
	if tag == "" {
		return params
	}
	var part string

	// Parse field name
	i := strings.IndexByte(tag, ',')
	if i < 0 {
		part, tag = tag, ""
	} else {
		part, tag = tag[:i], tag[i+1:]
	}
	params.name = part

	// Parse field parameters
	for len(tag) > 0 {
		// This loop uses IndexByte and explicit slicing
		// instead of strings.Split(str, ",") to reduce allocations.
		i = strings.IndexByte(tag, ',')
		if i < 0 {
			part, tag = tag, ""
		} else {
			part, tag = tag[:i], tag[i+1:]
		}
		switch {
		case part == "unnest":
			params.unnest = true
		case part == "omitempty":
			params.omitEmpty = true
		}
	}
	return params
}

type fieldParameters struct {
	unnest    bool
	omitEmpty bool
	name      string
}

// AppendParam append parameters for given @params.
func AppendParam(
	params []bigquery.QueryParameter,
	name string,
	value interface{},
) []bigquery.QueryParameter {
	return append(params, bigquery.QueryParameter{
		Name:  name,
		Value: value,
	})
}
