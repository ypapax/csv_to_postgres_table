package main

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/ypapax/csv_to_slice"
	"github.com/ypapax/yargs"
	"os"
	"strconv"
	"strings"
)

func main() {
	if err := func() error {
		fileName, base, content, err := yargs.FirstArgFileContent()
		if err != nil {
			return errors.WithStack(err)
		}
		tableName := strings.Split(base, ".")[0]
		result, err := fullWork(tableName, string(content))
		if err != nil {
			return errors.WithStack(err)
		}
		resultFile := fileName + ".result.sql"
		if err := os.WriteFile(resultFile, []byte(result), 0666); err != nil {
			return errors.WithStack(err)
		}
		logrus.Infof("result file is written: pbcopy < %+v", resultFile)
		return nil
	}(); err != nil {
		logrus.Fatalf("error: %+v", err)
	}
}

type CsvField struct {
	CsvFieldName string
	CsvVal       string
}

type PostgresField struct {
	name       string
	typeName   string
	defaultVal string
	notNull    bool
}

func csvFieldToPostgres(f CsvField) (p *PostgresField, err error) {
	if len(f.CsvFieldName) == 0 {
		return nil, errors.Errorf("empty")
	}
	return &PostgresField{name: strcase.ToSnake(f.CsvFieldName), typeName: postgresTypeByStrVal(f.CsvVal)}, nil
}

func postgresTypeByStrVal(s string) string {
	const defaultType = "text"
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultType
	}

	if _, err := strconv.ParseFloat(s, 10); err == nil {
		return "numeric"
	}
	if _, err := strconv.ParseBool(s); err == nil {
		return "bool"
	}
	return defaultType
}

func csvFieldsToPostgres(ff []CsvField) (pp []PostgresField, err error) {
	for _, f := range ff {
		p, err := csvFieldToPostgres(f)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if p == nil {
			return nil, errors.Errorf("expected not nil")
		}
		pp = append(pp, *p)
	}
	return pp, nil
}

func fullWork(tableName string, content string) (string, error) {
	csvFields, err := csvToFields(content)
	if err != nil {
		return "", errors.WithStack(err)
	}
	r, err := fieldsToCreateTablePostgresExpression(tableName, csvFields)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return r, nil
}

func fieldsToCreateTablePostgresExpression(tableName string, ff []CsvField) (string, error) {
	pp, err := csvFieldsToPostgres(ff)
	if err != nil {
		return "", errors.WithStack(err)
	}
	pp = append([]PostgresField{
		{name: "id", typeName: "bigserial", notNull: true},
		{name: "created_at", typeName: "timestamp", defaultVal: "NOW()", notNull: true},
		{name: "updated_at", typeName: "timestamp", defaultVal: "NOW()", notNull: true},
	}, pp...)
	createTableExpr, err := postgresFieldsToCreateTablePostgresExpression(tableName, pp)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return createTableExpr, nil
}

func postgresFieldsToCreateTablePostgresExpression(tableName string, pp []PostgresField) (string, error) {
	var lines []string
	lines = append(lines, fmt.Sprintf("CREATE TABLE IF NOT EXISTS "+tableName))
	lines = append(lines, "(")
	var fieldLines []string
	for _, p := range pp {
		l, err := postgresFieldToCreateTablePostgresExpressionLine(p)
		if err != nil {
			return "", errors.WithStack(err)
		}
		fieldLines = append(fieldLines, l)
	}
	lines = append(lines, strings.Join(fieldLines, ",\n"))
	lines = append(lines, "CONSTRAINT "+tableName+"_pk PRIMARY KEY (id),")
	lines = append(lines, ")")
	return strings.Join(lines, "\n"), nil
}

func postgresFieldToCreateTablePostgresExpressionLine(p PostgresField) (string, error) {
	if len(p.name) == 0 {
		return "", errors.Errorf("missing name")
	}
	if len(p.typeName) == 0 {
		return "", errors.Errorf("missing type name")
	}
	var parts = []string{
		p.name,
		p.typeName,
	}
	if len(p.defaultVal) > 0 {
		parts = append(parts, "DEFAULT", p.defaultVal)
	}
	if p.notNull {
		parts = append(parts, "NOT NULL")
	}
	return strings.Join(parts, " "), nil
	//ALTER TABLE "blogs" ADD "published" boolean DEFAULT FALSE NOT NULL
}

func csvToFields(csvStr string) ([]CsvField, error) {
	header, lines, err := csv_to_slice.CsvToSlice(csvStr, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(lines) == 0 {
		return nil, errors.Errorf("missing lines")
	}
	first := lines[0]
	if len(first) != len(header) {
		return nil, errors.Errorf("first line and header should be equal length")
	}
	var ff []CsvField
	for i, v := range first {
		ff = append(ff, CsvField{CsvFieldName: header[i], CsvVal: v})
	}
	return ff, nil
}
