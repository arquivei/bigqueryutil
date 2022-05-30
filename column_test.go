package bigqueryutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildColumnsClause(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		projection []string
		spec       QueryBuilderSpec
		expected   string
	}{
		{
			name:     "empty fields",
			expected: "*",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:       "empty fields",
			projection: []string{},
			expected:   "*",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:       "one field",
			projection: []string{"AccessKey"},
			expected:   "AccessKey",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:       "three fields",
			projection: []string{"AccessKey", "Owner", "OwnerRoles"},
			expected:   "AccessKey,Owner,OwnerRoles",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:       "array field",
			projection: []string{"Events.Date"},
			expected:   "ARRAY(SELECT AS STRUCT Date FROM UNNEST(Events)) AS Events",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:       "array field with struct field",
			projection: []string{"AccessKey", "NFe.infNFe.det.prod.CFOP", "OwnerRoles"},
			expected:   "AccessKey,STRUCT(STRUCT(ARRAY(SELECT AS STRUCT STRUCT(prod.CFOP) AS prod FROM UNNEST(NFe.infNFe.det)) AS det) AS infNFe) AS NFe,OwnerRoles",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events":                                                  {},
					"Manifestations":                                          {},
					"NFe.infNFe.ide.NFref":                                    {},
					"NFe.infNFe.autXML":                                       {},
					"NFe.infNFe.det":                                          {},
					"NFe.infNFe.det.prod.NVE":                                 {},
					"NFe.infNFe.det.prod.DI":                                  {},
					"NFe.infNFe.det.prod.DI.adi":                              {},
					"NFe.infNFe.det.prod.detExport":                           {},
					"NFe.infNFe.det.prod.rastro":                              {},
					"NFe.infNFe.det.prod.medLegacy":                           {},
					"NFe.infNFe.det.prod.arma":                                {},
					"NFe.infNFe.transp.reboque":                               {},
					"NFe.infNFe.transp.vol":                                   {},
					"NFe.infNFe.transp.vol.lacres":                            {},
					"NFe.infNFe.cobr.dup":                                     {},
					"NFe.infNFe.pag.detPag":                                   {},
					"NFe.infNFe.infAdic.obsCont":                              {},
					"NFe.infNFe.infAdic.obsFisco":                             {},
					"NFe.infNFe.infAdic.procRef":                              {},
					"NFe.infNFe.cana.forDia":                                  {},
					"NFe.infNFe.cana.deduc":                                   {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform": {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform.XPath": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.NotPanics(t, func() {
				columns := BuildColumnsClause(test.spec, test.projection)
				assert.Equal(t, test.expected, columns, test.name)
			})
		})
	}
}

func TestStructFieldBuilder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		in       []string
		expected string
		spec     QueryBuilderSpec
	}{
		{
			name:     "empty fields",
			expected: "",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "one simple field",
			in:       []string{"AccessKey"},
			expected: "AccessKey",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "two simple fields",
			in:       []string{"AccessKey", "Owner"},
			expected: "AccessKey,Owner",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "struct field with one level",
			in:       []string{"ResNFe.CNPJ"},
			expected: "STRUCT(ResNFe.CNPJ) AS ResNFe",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "struct field with three levels",
			in:       []string{"NFe.infNFe.emit.CNPJ"},
			expected: "STRUCT(STRUCT(STRUCT(NFe.infNFe.emit.CNPJ) AS emit) AS infNFe) AS NFe",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "simple and struct fields",
			in:       []string{"AccessKey", "ResNFe.CNPJ", "ResNFe.xNome", "NFe.infNFe.emit.CNPJ", "Owner", "NFe.infNFe.emit.xNome"},
			expected: "AccessKey,STRUCT(ResNFe.CNPJ,ResNFe.xNome) AS ResNFe,STRUCT(STRUCT(STRUCT(NFe.infNFe.emit.CNPJ,NFe.infNFe.emit.xNome) AS emit) AS infNFe) AS NFe,Owner",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "one array field",
			in:       []string{"Events.Date"},
			expected: "ARRAY(SELECT AS STRUCT Date FROM UNNEST(Events)) AS Events",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "array inside struct",
			in:       []string{"NFe.infNFe.det._nItem"},
			expected: "STRUCT(STRUCT(ARRAY(SELECT AS STRUCT _nItem FROM UNNEST(NFe.infNFe.det)) AS det) AS infNFe) AS NFe",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events":                                                  {},
					"Manifestations":                                          {},
					"NFe.infNFe.ide.NFref":                                    {},
					"NFe.infNFe.autXML":                                       {},
					"NFe.infNFe.det":                                          {},
					"NFe.infNFe.det.prod.NVE":                                 {},
					"NFe.infNFe.det.prod.DI":                                  {},
					"NFe.infNFe.det.prod.DI.adi":                              {},
					"NFe.infNFe.det.prod.detExport":                           {},
					"NFe.infNFe.det.prod.rastro":                              {},
					"NFe.infNFe.det.prod.medLegacy":                           {},
					"NFe.infNFe.det.prod.arma":                                {},
					"NFe.infNFe.transp.reboque":                               {},
					"NFe.infNFe.transp.vol":                                   {},
					"NFe.infNFe.transp.vol.lacres":                            {},
					"NFe.infNFe.cobr.dup":                                     {},
					"NFe.infNFe.pag.detPag":                                   {},
					"NFe.infNFe.infAdic.obsCont":                              {},
					"NFe.infNFe.infAdic.obsFisco":                             {},
					"NFe.infNFe.infAdic.procRef":                              {},
					"NFe.infNFe.cana.forDia":                                  {},
					"NFe.infNFe.cana.deduc":                                   {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform": {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform.XPath": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "array with struct inside struct",
			in:       []string{"NFe.infNFe.det._nItem", "NFe.infNFe.det.prod.CFOP", "NFe.infNFe.det.imposto.ICMS.ICMS00.vICMS"},
			expected: "STRUCT(STRUCT(ARRAY(SELECT AS STRUCT _nItem,STRUCT(prod.CFOP) AS prod,STRUCT(STRUCT(STRUCT(imposto.ICMS.ICMS00.vICMS) AS ICMS00) AS ICMS) AS imposto FROM UNNEST(NFe.infNFe.det)) AS det) AS infNFe) AS NFe",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events":                                                  {},
					"Manifestations":                                          {},
					"NFe.infNFe.ide.NFref":                                    {},
					"NFe.infNFe.autXML":                                       {},
					"NFe.infNFe.det":                                          {},
					"NFe.infNFe.det.prod.NVE":                                 {},
					"NFe.infNFe.det.prod.DI":                                  {},
					"NFe.infNFe.det.prod.DI.adi":                              {},
					"NFe.infNFe.det.prod.detExport":                           {},
					"NFe.infNFe.det.prod.rastro":                              {},
					"NFe.infNFe.det.prod.medLegacy":                           {},
					"NFe.infNFe.det.prod.arma":                                {},
					"NFe.infNFe.transp.reboque":                               {},
					"NFe.infNFe.transp.vol":                                   {},
					"NFe.infNFe.transp.vol.lacres":                            {},
					"NFe.infNFe.cobr.dup":                                     {},
					"NFe.infNFe.pag.detPag":                                   {},
					"NFe.infNFe.infAdic.obsCont":                              {},
					"NFe.infNFe.infAdic.obsFisco":                             {},
					"NFe.infNFe.infAdic.procRef":                              {},
					"NFe.infNFe.cana.forDia":                                  {},
					"NFe.infNFe.cana.deduc":                                   {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform": {},
					"NFe.Signature.SignedInfo.Reference.Transforms.Transform.XPath": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "two array fields from the same head element",
			in:       []string{"Events.Date", "Events.CreatedAt"},
			expected: "ARRAY(SELECT AS STRUCT Date,CreatedAt FROM UNNEST(Events)) AS Events",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
		{
			name:     "One of each supported kind (simple, struct and event)",
			in:       []string{"AccessKey", "NFe.infNFe.emit.CNPJ", "Events.Date"},
			expected: "AccessKey,STRUCT(STRUCT(STRUCT(NFe.infNFe.emit.CNPJ) AS emit) AS infNFe) AS NFe,ARRAY(SELECT AS STRUCT Date FROM UNNEST(Events)) AS Events",
			spec: QueryBuilderSpec{
				RepeatedColumns: map[string]struct{}{
					"Events": {},
				},
				SQLQuery: "SELECT * EXCEPT(r) FROM (SELECT %s, " +
					"ROW_NUMBER() OVER (PARTITION BY AccessKey, Owner order by Version desc) r " +
					"FROM %s WHERE %s%s) WHERE r = 1;",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.NotPanics(t, func() {
				b := columnsClauseBuilder{spec: test.spec}
				for _, f := range test.in {
					b.AddColumn(f)
				}
				sb := strings.Builder{}
				b.write(&sb)
				assert.Equal(t, test.expected, sb.String())
			})
		})
	}
}
