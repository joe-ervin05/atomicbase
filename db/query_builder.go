package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func (dao Database) SelectRows(req *http.Request) ([]byte, error) {
	params := req.URL.Query()
	table := req.PathValue("table")

	query := ""
	var args []any
	sel := ""

	if params["select"] != nil {
		sel = params["select"][0]
	}

	sel, err := buildSelect(sel, table, dao.Schema)
	if err != nil {
		return nil, err
	}

	query += sel

	where, wArgs, err := buildWhere(params)
	if err != nil {
		return nil, err
	}

	query += where
	args = append(args, wArgs...)

	if params["order"] != nil {
		orderBy, err := buildOrder(params["order"][0])
		if err != nil {
			return nil, err
		}

		query += orderBy
	}

	fmt.Println(query, args)

	return dao.QueryJson(query, args...)
}

func (dao Database) DeleteRows(req *http.Request) ([]byte, error) {

	table := req.PathValue("table")
	params := req.URL.Query()

	query := "DELETE FROM " + table + " "

	where, args, err := buildWhere(params)
	if err != nil {
		return nil, err
	}

	if where == "" {
		return nil, errors.New("all DELETES require a where clause")
	}
	query += where

	if params["select"] != nil {
		sel, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += sel

		if params["order"] != nil {
			order, err := buildOrder(params["order"][0])
			if err != nil {
				return nil, err
			}
			query += order
		}
	}

	return dao.QueryJson(query, args...)
}

func (dao Database) UpdateRows(req *http.Request) ([]byte, error) {
	table := req.PathValue("table")
	params := req.URL.Query()
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()

	var cols map[string]any
	err := dec.Decode(&cols)
	if err != nil {
		return nil, err
	}

	query, args, err := buildUpdate(cols, table)
	if err != nil {
		return nil, err
	}

	where, whereArgs, err := buildWhere(params)
	if err != nil {
		return nil, err
	}
	query += where
	args = append(args, whereArgs...)

	if params["select"] != nil {
		sel, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += sel

		if params["order"] != nil {
			order, err := buildOrder(params["order"][0])
			if err != nil {
				return nil, err
			}

			query += order
		}
	}

	return dao.QueryJson(query, args...)
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

func (dao Database) InsertRows(req *http.Request) ([]byte, error) {
	table := req.PathValue("table")
	params := req.URL.Query()
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()

	upsert := req.Header.Get("Prefer") == "resolution=merge-duplicates"

	query := ""
	var args []any

	if upsert {
		var cols []map[string]any
		pk := dao.Schema.Pks[table]

		err := dec.Decode(&cols)
		if err != nil {
			return nil, err
		}

		insert, insArgs, err := buildUpsert(cols, table, pk)
		if err != nil {
			return nil, err
		}

		query += insert
		args = append(args, insArgs...)

	} else {
		var cols map[string]any

		err := dec.Decode(&cols)
		if err != nil {
			return nil, err
		}

		insert, insArgs, err := buildInsert(cols, table)
		if err != nil {
			return nil, err
		}

		query += insert
		args = append(args, insArgs...)
	}

	if params["select"] != nil {
		sel, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += sel

		if params["order"] != nil {
			order, err := buildOrder(params["order"][0])
			if err != nil {
				return nil, err
			}

			query += order
		}
	}

	return dao.QueryJson(query, args...)
}

func buildUpsert(colSlice []map[string]any, table string, pk string) (string, []any, error) {

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

		query += fmt.Sprintf("LEFT JOIN %s on %s.%s = %s.%s ", fk.Table, fk.References, fk.To, fk.Table, fk.From)
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
		if currTable == table {
			cols = append(cols, currStr)
		} else {
			cols = append(cols, dotSeparate(currTable, currStr))
		}
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

func buildWhere(params url.Values) (string, []any, error) {

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

		keys := splitNotQuotes(val[0], '.')

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
