package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

func (dao Database) SelectRows(req *http.Request) ([]interface{}, error) {
	params := req.URL.Query()
	table := req.PathValue("table")

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

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

	where, wArgs, err := buildWhere(table, dao.Schema.Tables[table], params)
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

	return dao.QueryMap(query, args...)
}

func (dao Database) DeleteRows(req *http.Request) ([]interface{}, error) {

	table := req.PathValue("table")
	params := req.URL.Query()

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	query := "DELETE FROM " + table + " "

	where, args, err := buildWhere(table, dao.Schema.Tables[table], params)
	if err != nil {
		return nil, err
	}

	if where == "" {
		return nil, errors.New("all DELETES require a where clause")
	}
	query += where

	if params["select"] != nil {
		selQuery, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		return dao.QueryMap(query, args...)
	}

	_, err = dao.client.Exec(query, args...)

	return nil, err
}

func (dao Database) InsertRows(req *http.Request) ([]interface{}, error) {
	table := req.PathValue("table")

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

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
		selQuery, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		return dao.QueryMap(query, args...)
	}

	_, err := dao.client.Exec(query, args...)

	return nil, err
}

func (dao Database) UpdateRows(req *http.Request) ([]interface{}, error) {
	table := req.PathValue("table")

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

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

	where, whereArgs, err := buildWhere(table, dao.Schema.Tables[table], params)
	if err != nil {
		return nil, err
	}
	query += where
	args = append(args, whereArgs...)

	if params["select"] != nil {
		selQuery, err := buildReturning(params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		if params["order"] != nil {
			order, err := buildOrder(params["order"][0])
			if err != nil {
				return nil, err
			}

			query += order
		}

		return dao.QueryMap(query, args...)
	}

	_, err = dao.client.Exec(query, args...)

	return nil, err
}

func buildUpdate(cols map[string]any, table string) (string, []any, error) {

	query := "UPDATE [" + table + "] SET "
	args := make([]any, len(cols))

	colI := 0
	for col, val := range cols {
		query += col + " = ?, "
		args[colI] = val
		colI++
	}

	return query[:len(query)-2] + " ", nil, nil
}

func buildUpsert(colSlice []map[string]any, table string, pk string) (string, []any, error) {

	query := "INSERT INTO [" + table + "] ( "
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

	query := "INSERT INTO [" + table + "] "
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

		if fk == (Fk{}) {
			return "", fmt.Errorf("no relationship exists between %s and %s. This may be because of a stale schema cache. use POST /api/schema/invalidate to refresh the cache", tbl, ref)
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

	var cols []string
	quoted := false
	currTable := table
	var prevTable string
	currStr := ""
	escaped := false
	alias := ""
	_ = alias

	for _, v := range str {
		if escaped {
			currStr += string(v)
			escaped = false
			continue
		}
		if v == '\\' {
			escaped = true
			continue
		}
		if quoted && v != '"' {
			currStr += string(v)
			continue
		}

		switch v {
		case '"':
			quoted = !quoted
		case '(':
			prevTable = currTable
			currTable = currStr
			currStr = ""
			alias = ""
			fkMap[currTable] = prevTable
		case ')':
			if currStr != "" {
				fullCol := dotSeparate(currTable, currStr)
				if alias != "" {
					fullCol += " AS " + alias + " "
				}
				cols = append(cols, fullCol)
				currStr = ""
			}
			alias = ""
			currTable = prevTable
		case ':':
			alias = currStr
			currStr = ""
		case ',':
			if currTable == table {
				if alias != "" {
					currStr += " AS " + alias
				}
				cols = append(cols, currStr)
			} else {
				fullCol := dotSeparate(currTable, currStr)
				if alias != "" {
					fullCol += " AS " + alias
				}
				cols = append(cols, fullCol)
			}
			alias = ""
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

	orderBy := splitAtomic(param, ',')

	for i, col := range orderBy {
		keys := splitAtomic(col, '.')

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

	keys := splitAtomic(param, ',')

	for i, key := range keys {
		if i == len(keys)-1 {
			query += key + " "
		} else {
			query += key + ", "
		}
	}

	return query, nil
}

func buildWhere(table string, schemaCols map[string]string, params url.Values) (string, []any, error) {

	var args []any
	query := "Where "
	hasWhere := false
	i := 0

	for name, val := range params {
		if name == "or" {
			if len(val[0]) > 2 {
				orParams := splitParenthesis(val[0][1 : len(val[0])-1])
				for _, ops := range orParams {
					if i != 0 {
						query += "OR "
					}

					query += "[" + ops[0] + "]" + " "
					for i = 1; i < len(ops); i++ {
						if mapOperator(ops[i]) != "" {
							query += mapOperator(ops[i]) + " "
						} else {
							query += "? "
							args = append(args, ops[i])
						}
					}
					i++
				}

				hasWhere = true
			}
			continue
		}

		if name != "order" && name != "select" {
			normalizedParam := unQuoteParam(name)
			if schemaCols[normalizedParam] == "" {
				return "", nil, invalidColErr(name, table)
			}

			hasWhere = true

			if i != 0 {
				query += "AND "
			}

			query += "[" + normalizedParam + "]" + " "

			keys := splitAtomic(val[0], '.')

			for _, v := range keys {
				if mapOperator(v) != "" {
					query += mapOperator(v) + " "
				} else {
					query += "? "

					args = append(args, v)
				}
			}
			i++
		}
	}

	if !hasWhere {
		return "", nil, nil
	}

	return query, args, nil
}

func splitParenthesis(s string) [][]string {
	inQuotes := false
	var fullList [][]string
	var currList []string
	currStr := ""
	escaped := false

	for _, v := range s {
		if escaped {
			currStr += string(v)
			escaped = false
		} else if v == '\\' {
			escaped = true
		} else if v == '"' {
			inQuotes = !inQuotes
		} else if v == ',' && !inQuotes {
			currList = append(currList, currStr)
			fullList = append(fullList, currList)
			currList = nil
			currStr = ""
		} else if v == '.' && !inQuotes {
			currList = append(currList, currStr)
			currStr = ""
		} else {
			currStr += string(v)
		}
	}

	if currStr != "" {
		currList = append(currList, currStr)
	}
	if currList != nil {
		fullList = append(fullList, currList)
	}

	return fullList
}

func unQuoteParam(param string) string {
	escaped := false
	newStr := ""
	for _, v := range param {
		if escaped {
			newStr += string(v)
			escaped = false
		} else if v == '\\' {
			escaped = true
		} else if v != '"' {
			newStr += string(v)
		}
	}

	return newStr
}

func splitAtomic(s string, delimiter rune) []string {
	inQuotes := false
	var list []string
	currStr := ""
	escaped := false

	for _, v := range s {
		if escaped {
			currStr += string(v)
			escaped = false
			continue
		}

		if v == '\\' {
			escaped = true
			continue
		}

		if v == '"' {
			inQuotes = !inQuotes
			continue
		}

		if v == delimiter && !inQuotes {
			list = append(list, currStr)
			currStr = ""
			continue
		}

		currStr += string(v)
	}

	if currStr != "" {
		list = append(list, currStr)
	}

	return list
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