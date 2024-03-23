package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
)

func DeleteRows(params map[string]string, table string) (string, []any, error) {

	query := "DELETE FROM " + table + " "

	where, args, err := buildWhere(params)
	if err != nil {
		return "", nil, err
	}

	if where == "" {
		return "", nil, errors.New("all DELETES require a where clause")
	}
	query += where

	if params["select"] != "" {
		sel, err := buildReturning(params["select"])
		if err != nil {
			return "", nil, err
		}

		query += sel

		if params["order"] != "" {
			order, err := buildOrder(params["order"])
			if err != nil {
				return "", nil, err
			}
			query += order
		}
	}

	return query, args, nil
}

func UpdateRows(c fiber.Ctx) (string, []any, error) {
	table := c.Params("table")
	params := c.Queries()
	body := c.Request().Body()

	var cols map[string]any
	err := json.Unmarshal(body, &cols)
	if err != nil {
		return "", nil, err
	}

	query, args, err := buildUpdate(cols, table)
	if err != nil {
		return "", nil, err
	}

	where, whereArgs, err := buildWhere(params)
	if err != nil {
		return "", nil, err
	}
	query += where
	args = append(args, whereArgs...)

	if params["select"] != "" {
		sel, err := buildReturning(params["select"])
		if err != nil {
			return "", nil, err
		}

		query += sel

		if params["order"] != "" {
			order, err := buildOrder(params["order"])
			if err != nil {
				return "", nil, err
			}

			query += order
		}
	}

	return query, args, nil
}

func buildUpdate(cols map[string]any, table string) (string, []any, error) {

	query := "UPDATE " + table + " SET "
	args := make([]any, len(cols))

	colI := 0
	for col, val := range cols {
		query += col + " = ?, "
		args[colI] = val
		colI++
	}

	return query[:len(query)-2] + " ", nil, nil
}

func InsertRows(c fiber.Ctx) (string, []any, error) {
	table := c.Params("table")
	params := c.Queries()
	body := c.Request().Body()
	upsert := c.Get("Prefer") == "resolution=merge-duplicates"

	query := ""
	var args []any

	if upsert {
		type columnSlice []map[string]any
		var cols columnSlice
		pk := c.Locals("schema").(SchemaCache).Pks[table]

		err := json.Unmarshal(body, &cols)
		if err != nil {
			return "", nil, err
		}

		insert, insArgs, err := buildUpsert(cols, table, pk)
		if err != nil {
			return "", nil, err
		}

		query += insert
		args = append(args, insArgs...)

	} else {
		type columns map[string]any
		var cols columns

		err := json.Unmarshal(body, &cols)
		if err != nil {
			return "", nil, err
		}

		insert, insArgs, err := buildInsert(cols, table)
		if err != nil {
			return "", nil, err
		}

		query += insert
		args = append(args, insArgs...)
	}

	if params["select"] != "" {
		sel, err := buildReturning(params["select"])
		if err != nil {
			return "", nil, err
		}

		query += sel

		if params["order"] != "" {
			order, err := buildOrder(params["order"])
			if err != nil {
				return "", nil, err
			}

			query += order
		}
	}

	return query, args, nil
}

func buildUpsert(colSlice []map[string]any, table string, pk string) (string, []any, error) {
	fmt.Println(pk)

	query := "INSERT INTO " + table + " ( "
	args := make([]any, len(colSlice)*len(colSlice[0]))
	valHolder := "( "

	colI := 0
	for col := range colSlice[0] {
		query += col + ", "
		valHolder += "?, "

		for i, cols := range colSlice {

			args[i*len(cols)+colI] = cols[col]

		}

		colI++
	}

	valHolder = valHolder[:len(valHolder)-2] + "), "

	// gets rid of the last comma
	query = query[:len(query)-2] + " ) VALUES "

	for i := 0; i < len(colSlice); i++ {
		query += valHolder

	}

	query = query[:len(query)-2] + fmt.Sprintf(" ON CONFLICT(%s) DO UPDATE SET ", pk)

	for col := range colSlice[0] {
		if col != pk {
			query += col + " = excluded." + col + ", "
		}
	}

	return query[:len(query)-2] + " ", args, nil

}

func SelectRows(c fiber.Ctx) (string, []any, error) {
	params := c.Queries()
	table := c.Params("table")
	schema := c.Locals("schema").(SchemaCache)

	query := ""
	var args []any

	sel, err := buildSelect(params["select"], table, schema)
	if err != nil {
		return "", nil, err
	}

	query += sel

	where, wArgs, err := buildWhere(params)
	if err != nil {
		return "", nil, err
	}

	query += where
	args = append(args, wArgs...)

	orderBy, err := buildOrder(params["order"])
	if err != nil {
		return "", nil, err
	}

	query += orderBy

	return query, args, nil
}

func buildReturning(param string) (string, error) {

	query := "RETURNING "

	keys := splitNotQuotes(param, ',')

	for i, key := range keys {

		if i == len(keys)-1 {
			query += key + " "
		} else {
			query += key + ", "
		}
	}

	return query, nil
}

func buildInsert(cols map[string]any, table string) (string, []any, error) {

	query := "INSERT INTO " + table + " "
	args := make([]any, len(cols))

	i := 0
	columns := "( "
	values := "( "

	for col, val := range cols {
		args[i] = val
		columns += col + ", "
		values += "?, "
		i++
	}

	columns = columns[:len(columns)-2] + " ) "
	values = values[:len(values)-2] + ") "

	query += columns + "VALUES " + values

	return query, args, nil

}

func buildSelect(param string, table string, schema SchemaCache) (string, error) {

	cols, rels, err := parseSelect(param, table)
	if err != nil {
		return "", err
	}

	query := "SELECT "

	for _, col := range cols {
		query += col + ", "
	}

	query = query[:len(query)-2] + " FROM " + table + " "

	if len(rels) == 0 {
		return query, nil
	}

	for tbl, ref := range rels {
		var fk Fk

		for _, val := range schema.Fks {
			if val.Table == tbl && val.References == ref {
				fk = val
				break
			}
		}

		fmt.Println(fk)
	}

	return query, nil
}

func parseSelect(str string, table string) ([]string, map[string]string, error) {

	fkMap := make(map[string]string)

	if str == "" {
		cols := make([]string, 1)
		cols[0] = "*"
		return cols, nil, nil
	}

	if strings.Count(str, "\"")%2 != 0 {
		return nil, nil, errors.New("the requested select query is not parsable because of unclosed quotation marks")
	}

	var cols []string
	inQuotes := false
	currTable := table
	var prevTable string
	currStr := ""

	for _, v := range str {
		if inQuotes && v != '"' {
			currStr += string(v)
			continue
		}

		switch v {
		case '"':
			inQuotes = !inQuotes
		case '(':
			prevTable = currTable
			currTable = currStr
			currStr = ""
			fkMap[currTable] = prevTable
		case ')':
			cols = append(cols, dotSeparate(currTable, currStr))
			currTable = prevTable
		case ',':
			if currTable == table {
				cols = append(cols, currStr)
			} else {
				cols = append(cols, dotSeparate(currTable, currStr))
			}
			currStr = ""
		default:
			currStr += string(v)
		}
	}

	if currStr != "" {
		cols = append(cols, dotSeparate(currTable, currStr))
	}

	return cols, fkMap, nil
}

func buildOrder(param string) (string, error) {
	if param == "" {
		return "", nil
	}

	query := "ORDER BY "

	orderBy := splitNotQuotes(param, ',')

	for i, col := range orderBy {
		keys := splitNotQuotes(col, '.')

		if len(keys) > 1 {

			if keys[1] == "desc" {
				keys[1] = "DESC"
			} else if keys[1] == "asc" {
				keys[1] = "ASC"
			} else {
				return "", errors.New("unknown sorting for order by")
			}
		}

		if i != len(orderBy)-1 {

			if len(keys) == 2 {
				query += fmt.Sprintf("%s %s, ", keys[0], keys[1])
			} else {
				query += keys[0] + ", "
			}

		} else {

			if len(keys) == 2 {
				query += fmt.Sprintf("%s %s ", keys[0], keys[1])
			} else {
				query += keys[0] + " "
			}

		}

	}

	return query, nil
}

func buildWhere(params map[string]string) (string, []any, error) {

	var args []any
	query := "Where "
	hasWhere := false

	for name, val := range params {
		if name != "order" && name != "select" {
			hasWhere = true
		} else {
			continue
		}

		query += name + " "

		keys := splitNotQuotes(val, '.')

		for _, v := range keys {
			if mapOperator(v) != "" {
				query += mapOperator(v) + " "
			} else {
				query += "? "
				args = append(args, v)
			}
		}

	}

	if !hasWhere {
		return "", nil, nil
	}

	return query, args, nil
}

func splitNotQuotes(s string, delimiter rune) []string {
	inQuotes := false

	return strings.FieldsFunc(s, func(r rune) bool {
		if r == '"' {
			inQuotes = !inQuotes
			return false
		}

		if r == delimiter {
			return true
		}

		return false
	})
}

func mapOperator(str string) string {

	operators := map[string]string{
		"eq":      "=",
		"lt":      "<",
		"gt":      ">",
		"lte":     "<=",
		"gte":     ">=",
		"neq":     "!=",
		"like":    "LIKE",
		"glob":    "GLOB",
		"between": "BETWEEN",
		"not":     "NOT",
		"in":      "IN",
		"is":      "IS",
		"and":     "AND",
		"or":      "OR",
	}

	return operators[str]
}

func dotSeparate(x, y string) string {
	if x == "" {
		return y
	}
	if y == "" {
		return x
	}

	return x + "." + y
}
