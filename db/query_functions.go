package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type Sel struct {
	column string
	alias  string
}

type SelMap map[string][]Sel

type relation struct {
	from  string
	to    string
	alias string
}

func (dao Database) SelectRows(req *http.Request) ([]byte, error) {
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

	sel, agg, err := dao.Schema.buildSelect(sel, table)
	if err != nil {
		return nil, err
	}

	query += sel

	where, wArgs, err := dao.Schema.buildWhere(table, params)
	if err != nil {
		return nil, err
	}

	query += where
	args = append(args, wArgs...)

	query += fmt.Sprintf("GROUP BY [%s].[%s] ", table, dao.Schema.Pks[table])

	if params["order"] != nil {
		orderBy, err := dao.Schema.buildOrder(table, params["order"][0])
		if err != nil {
			return nil, err
		}

		query += orderBy
	}

	row := dao.client.QueryRow(fmt.Sprintf("SELECT json_group_array(json_object(%s)) AS data FROM (%s)", agg, query), args...)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var res []byte

	err = row.Scan(&res)

	return res, err
}

func (dao Database) DeleteRows(req *http.Request) ([]byte, error) {

	table := req.PathValue("table")
	params := req.URL.Query()

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	query := "DELETE FROM [" + table + "] "

	where, args, err := dao.Schema.buildWhere(table, params)
	if err != nil {
		return nil, err
	}

	if where == "" {
		return nil, errors.New("all DELETES require a where clause")
	}
	query += where

	if params["select"] != nil {
		selQuery, err := dao.Schema.buildReturning(table, params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		return dao.QueryJSON(query, args...)
	}

	_, err = dao.client.Exec(query, args...)

	return nil, err
}

func (dao Database) InsertRows(req *http.Request) ([]byte, error) {
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
		selQuery, err := dao.Schema.buildReturning(table, params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		return dao.QueryJSON(query, args...)
	}

	_, err := dao.client.Exec(query, args...)

	return nil, err
}

func (dao Database) UpdateRows(req *http.Request) ([]byte, error) {
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

	query := "UPDATE [" + table + "] SET "
	args := make([]any, len(cols))

	colI := 0
	for col, val := range cols {

		if dao.Schema.Tables[table][col] == "" {
			return nil, InvalidColErr(col, table)
		}

		if colI == len(cols)-1 {
			query += fmt.Sprintf("[%s] = ? ", col)
		} else {
			query += fmt.Sprintf("[%s] = ?, ", col)
		}
		args[colI] = val
		colI++
	}

	where, whereArgs, err := dao.Schema.buildWhere(table, params)
	if err != nil {
		return nil, err
	}
	query += where
	args = append(args, whereArgs...)

	if params["select"] != nil {
		selQuery, err := dao.Schema.buildReturning(table, params["select"][0])
		if err != nil {
			return nil, err
		}

		query += selQuery

		return dao.QueryJSON(query, args...)
	}

	_, err = dao.client.Exec(query, args...)

	return nil, err
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

func (schema SchemaCache) buildSelect(param string, table string) (string, string, error) {

	if param == "" || param == "*" {
		return "SELECT * FROM " + table, "", nil
	}

	sels, rels, err := schema.parseSelect(param, table)
	if err != nil {
		return "", "", err
	}

	query := "SELECT "

	jsn, agg := schema.buildJSONSel(table, sels)

	query += jsn + fmt.Sprintf(" FROM [%s] ", table)

	for _, rel := range rels {

		var fk Fk

		for _, key := range schema.Fks {
			if key.Table == rel.from && key.References == rel.to {
				fk = key
				break
			}
		}

		if fk == (Fk{}) {
			return "", "", fmt.Errorf("no relationship exists between %s and %s. This may be because of a stale schema cache. use POST /schema/invalidate to refresh the cache", rel.from, rel.to)
		}

		query += fmt.Sprintf("LEFT JOIN [%s] on [%s].[%s] = [%s].[%s] ", fk.Table, fk.References, fk.To, fk.Table, fk.From)
	}

	return query, agg, nil
}

func (schema SchemaCache) buildJSONSel(table string, sels SelMap) (string, string) {
	var aggregate string
	query := ""
	for _, sel := range sels[table] {
		query += fmt.Sprintf("[%s].[%s]", table, sel.column)
		if sel.alias != "" {
			aggregate += fmt.Sprintf("'%s', [%s], ", sel.alias, sel.alias)
			query += fmt.Sprintf(" AS [%s]", sel.alias)
		} else {
			aggregate += fmt.Sprintf("'%s', [%s], ", sel.column, sel.column)
		}
		query += ", "
	}

	sels[table] = nil

	for name, cols := range sels {
		if cols == nil {
			continue
		}
		aggregate += fmt.Sprintf("'%s', [%s], ", name, name)
		query += "json_group_array(json_object("
		for _, sel := range cols {
			if sel.column == "*" {

			}

			if sel.alias != "" {
				query += fmt.Sprintf("'%s', [%s].[%s], ", sel.alias, name, sel.column)
			} else {
				query += fmt.Sprintf("'%s', [%s].[%s], ", sel.column, name, sel.column)
			}
		}

		query = query[:len(query)-2] + fmt.Sprintf(")) FILTER(WHERE [%s].[%s] IS NOT NULL) AS [%s], ", name, schema.Pks[name], name)
	}

	return query[:len(query)-2], aggregate[:len(aggregate)-2]
}

func (schema SchemaCache) parseSelect(param string, table string) (SelMap, []relation, error) {
	sels := make(SelMap)
	var rels []relation
	var prevTable string
	currTable := table
	currStr := ""
	alias := ""
	quoted := false
	escaped := false

	for _, v := range param {
		if escaped {
			currStr += string(v)
			escaped = false
			continue
		} else if v == '\\' {
			escaped = true
			continue
		} else if quoted && v != '"' {
			currStr += string(v)
			continue
		} else {
			switch v {
			case '"':
				quoted = !quoted
			case '(':
				if schema.Tables[currStr] == nil {
					return nil, nil, InvalidTblErr(currStr)
				}

				prevTable = currTable
				currTable = currStr
				currStr = ""
				rels = append(rels, relation{currTable, prevTable, alias})
				alias = ""
			case ')':
				if currStr != "" {
					if currStr != "*" && schema.Tables[currTable][currStr] == "" {
						return nil, nil, InvalidColErr(currStr, currTable)
					}
					sels[currTable] = append(sels[currTable], Sel{currStr, alias})
					currStr = ""
				}
				alias = ""
				currTable = prevTable
			case ':':
				alias = currStr
				currStr = ""
			case ',':
				if currStr != "" {
					if currStr != "*" && schema.Tables[currTable][currStr] == "" {
						return nil, nil, InvalidColErr(currStr, currTable)
					}
					sels[currTable] = append(sels[currTable], Sel{currStr, alias})
					alias = ""
					currStr = ""
				}
			default:
				currStr += string(v)
			}
		}
	}

	if currStr != "" {
		if currStr != "*" && schema.Tables[currTable][currStr] == "" {
			return nil, nil, InvalidColErr(currStr, currTable)
		}
		sels[currTable] = append(sels[currTable], Sel{currStr, alias})
	}

	return sels, rels, nil
}

func (schema SchemaCache) buildOrder(table, param string) (string, error) {
	if param == "" {
		return "", nil
	}

	query := "ORDER BY "

	orderBy := splitParenthesis(param)

	for i, col := range orderBy {
		for _, op := range col {
			if op == "desc" {
				query += "DESC "
			} else if op == "asc" {
				query += "ASC "
			} else if schema.Tables[table][op] != "" {
				query += fmt.Sprintf("[%s] ", op)
			} else {
				return "", InvalidColErr(op, table)
			}
		}

		if i < len(col)-1 {
			query += ", "
		}
	}

	return query, nil
}

func (schema SchemaCache) buildReturning(table, param string) (string, error) {
	if param == "*" {
		return "RETURNING *", nil
	}

	query := "RETURNING "

	keys := splitAtomic(param, ',')

	for i, key := range keys {
		if schema.Tables[table][key] == "" {
			return "", InvalidColErr(param, table)
		}

		if i == len(keys)-1 {
			query += fmt.Sprintf("[%s] ", key)
		} else {
			query += fmt.Sprintf("[%s], ", key)
		}
	}

	return query, nil
}

func (schema SchemaCache) buildWhere(table string, params url.Values) (string, []any, error) {

	var args []any
	query := "Where "
	hasWhere := false
	i := 0

	for name, val := range params {
		if name != "order" && name != "select" {
			splitParam := splitAtomic(name, '.')
			if len(splitParam) == 1 {
				splitParam = []string{table, splitParam[0]}
			}

			if schema.Tables[splitParam[0]][splitParam[1]] == "" {
				return "", nil, InvalidColErr(splitParam[1], splitParam[0])
			}

			hasWhere = true

			if i != 0 {
				query += "AND "
			}

			query += fmt.Sprintf("[%s].[%s] ", splitParam[0], splitParam[1])

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
		return fmt.Sprintf("[%s]", y)
	}
	if y == "" {
		return fmt.Sprintf("[%s]", x)
	}

	return fmt.Sprintf("[%s].[%s]", x, y)
}
