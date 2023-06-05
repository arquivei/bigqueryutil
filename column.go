package bigqueryutil

import (
	"io"
	"strings"
)

const (
	columnNameSegmentString = iota
	columnNameSegmentStruct
	columnNameSegmentArray
)

// columnNameSegment holds a segment of the column name and a reference to the builder of the rest of the name.
// If builder is nil, name should hold the full name of the column.
type columnNameSegment struct {
	// Type of the segment: string, array or struct
	_type int
	// The name of the current segment, examples: AccessKey,imposto, det, xNome
	name string
	// If this segment is an array or a struct it will have children segments
	childrenBuilder *columnsClauseBuilder
	// The parent segment of this segment
	parent *columnNameSegment
}

// fullnameInsideArray will return the name of the segment pre-appended with its parent fullnameInsideArray if exists.
// But if the parent is an array it returns its current name because thats just how it works
// Examples:
//
//	AccessKey -> AccessKey
//	NFe.infNFe -> NFe.infNFe
//	NFe.infNFe.ide.nNF -> NFe.infNFe.ide.nNF
//	Events.Date -> Date
//	NFe.infNFe.det.imposto.ICMS.ICMS00.vICMS -> imposto.ICMS.ICMS00.vICMS
func (cns *columnNameSegment) fullnameInsideArray() string {
	if cns.parent != nil && cns.parent._type != columnNameSegmentArray {
		return cns.parent.fullnameInsideArray() + "." + cns.name
	}
	return cns.name
}

// fullname will return the name of the segment pre-appended with its parent fullnameif exists.
// Similar to fullnameInsideArray, but correctly handles the full path.
func (cns *columnNameSegment) fullname() string {
	if cns.parent != nil {
		return cns.parent.fullname() + "." + cns.name
	}
	return cns.name
}

// columnsClauseBuilder takes columns names in it's string form like "NFe.infNFe.emit.CNPJ" and builds
// a bigquery columns clause with complex fields as structs, like "struct(struct(struct(NFe.infNFe.emit.CNPJ) AS emit) AS infNFe) AS NFe".
type columnsClauseBuilder struct {
	columns []*columnNameSegment
	parent  *columnNameSegment
	spec    QueryBuilderSpec
}

// AddColumn takes a columns name.
func (b *columnsClauseBuilder) AddColumn(c string) {
	s := strings.Split(c, ".")
	b.addColumn(s[0], s[1:])
}

// addColumn takes the parent segment, the remainder of the segment and the full name
// parses is and append to the list of columns.
func (b *columnsClauseBuilder) addColumn(head string, tail []string) {
	if len(tail) == 0 {
		b.columns = append(b.columns, newStringSegment(b.parent, head))
		return
	}

	b.getOrCreateExistingSegment(head).
		childrenBuilder.addColumn(tail[0], tail[1:])
}

func (b *columnsClauseBuilder) getOrCreateExistingSegment(name string) *columnNameSegment {
	segment := b.getSegment(name)
	if segment == nil {
		segment = newArrayOrStructSegment(b.spec, b.parent, name)
		b.columns = append(b.columns, segment)
	}
	return segment
}

func newStringSegment(parent *columnNameSegment, name string) *columnNameSegment {
	return &columnNameSegment{
		_type:  columnNameSegmentString,
		name:   name,
		parent: parent,
	}
}

// getArrayOrStructSegmentType will either return an array segment or a struct segment
// based on its name.
func getArrayOrStructSegmentType(spec QueryBuilderSpec, fullname string) int {
	if isRepeated(spec, fullname) {
		return columnNameSegmentArray
	}
	return columnNameSegmentStruct
}

func newArrayOrStructSegment(spec QueryBuilderSpec, parent *columnNameSegment, name string) *columnNameSegment {
	segment := &columnNameSegment{
		name:   name,
		parent: parent,
	}
	segment._type = getArrayOrStructSegmentType(spec, segment.fullname())
	segment.childrenBuilder = &columnsClauseBuilder{
		spec:   spec,
		parent: segment,
	}
	return segment
}

func (b *columnsClauseBuilder) getSegment(name string) *columnNameSegment {
	for i := range b.columns {
		if b.columns[i].name == name {
			return b.columns[i]
		}
	}
	return nil
}

// nolint: errcheck
// write writes all added columns to the string writer.
func (b *columnsClauseBuilder) write(w io.StringWriter) {
	for i, c := range b.columns {
		if i > 0 {
			w.WriteString(",")
		}
		switch c._type {
		case columnNameSegmentString:
			w.WriteString(c.fullnameInsideArray())
		case columnNameSegmentStruct:
			w.WriteString("STRUCT(")
			c.childrenBuilder.write(w)
			w.WriteString(") AS ")
			w.WriteString(c.name)
		case columnNameSegmentArray:
			w.WriteString("ARRAY(SELECT AS STRUCT ")
			c.childrenBuilder.write(w)
			w.WriteString(" FROM UNNEST(")
			w.WriteString(c.fullnameInsideArray())
			w.WriteString(")) AS ")
			w.WriteString(c.name)
		default:
			panic("unknown columnNameSegment")
		}
	}
}

func (b *columnsClauseBuilder) String() string {
	w := &strings.Builder{}
	b.write(w)
	return w.String()
}

func isRepeated(spec QueryBuilderSpec, field string) bool {
	_, ok := spec.RepeatedColumns[field]
	return ok
}

// BuildColumnsClause builds a column clause.
func BuildColumnsClause(spec QueryBuilderSpec, projection []string) string {
	// Sanity check.
	// The projection fields are a required field on the HTTP API.
	if len(projection) == 0 {
		return "*"
	}

	cb := columnsClauseBuilder{spec: spec}

	for _, f := range projection {
		cb.AddColumn(f)
	}

	return cb.String()
}
