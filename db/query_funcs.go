package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
)

type Table struct {
	name    string
	columns []column
	joins   []*Table
	parent  *Table
}

type column struct {
	name  string
	alias string
}

func (dao Database) SelectRows(table string, params url.Values) ([]byte, error) {
	if dao.id == 1 && table == "databases" {
		return nil, errors.New("table databases is not queryable")
	}

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	query := ""
	var args []any
	sel := ""

	if params["select"] != nil {
		sel = params["select"][0]

		if sel == "" {
			sel = "*"
		}
	} else {
		sel = "*"
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

	fmt.Printf("SELECT json_group_array(json_object(%s)) AS data FROM (%s)", agg, query)
	fmt.Println(args)

	row := dao.Client.QueryRow(fmt.Sprintf("SELECT json_group_array(json_object(%s)) AS data FROM (%s)", agg, query), args...)
	if row.Err() != nil {
		return nil, row.Err()
	}

	var res []byte

	err = row.Scan(&res)

	return res, err
}

func (dao Database) DeleteRows(table string, params url.Values) ([]byte, error) {

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	query := fmt.Sprintf("DELETE FROM [%s] ", table)

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

	_, err = dao.Client.Exec(query, args...)

	return nil, err
}

func (dao Database) InsertRows(table string, params url.Values, body io.ReadCloser, upsert bool) ([]byte, error) {

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	dec := json.NewDecoder(body)

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

	_, err := dao.Client.Exec(query, args...)

	return nil, err
}

func (dao Database) UpdateRows(table string, params url.Values, body io.ReadCloser) ([]byte, error) {

	if dao.Schema.Tables[table] == nil {
		return nil, InvalidTblErr(table)
	}

	dec := json.NewDecoder(body)
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

	_, err = dao.Client.Exec(query, args...)

	return nil, err
}

func buildUpsert(colSlice []map[string]any, table string, pk string) (string, []any, error) {

	query := "INSERT INTO [" + table + "] ( "
	args := make([]any, len(colSlice)*len(colSlice[0]))
	valHolder := "( "

	colI := 0
	for col := range colSlice[0] {
		query += fmt.Sprintf("[%s], ", col)
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
			query += col + " = excluded.[" + col + "], "
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

	tbls, err := schema.parseSelect(param, table)
	if err != nil {
		return "", "", err
	}

	return schema.buildOuterAgg(tbls)
}

func (schema SchemaCache) buildOuterAgg(table Table) (string, string, error) {
	agg := ""
	sel := ""
	joins := ""

	if table.columns == nil && table.joins == nil {
		table.columns = []column{{"*", ""}}
	}

	for _, col := range table.columns {
		if col.name == "*" {
			sel += "*, "
			for name := range schema.Tables[table.name] {
				agg += fmt.Sprintf("'%s', [%s], ", name, name)
			}

			continue
		}

		sel += fmt.Sprintf("[%s].[%s], ", table.name, col.name)
		if col.alias != "" {
			agg += fmt.Sprintf("'%s', [%s], ", col.alias, col.name)
		} else {
			agg += fmt.Sprintf("'%s', [%s], ", col.name, col.name)
		}
	}

	for _, tbl := range table.joins {
		agg += fmt.Sprintf("'%s', json([%s]), ", tbl.name, tbl.name)
		query, aggs, err := schema.buildSelCurr(*tbl, table.name)
		if err != nil {
			return "", "", err
		}
		var fk Fk
		for _, key := range schema.Fks {
			if key.References == table.name && key.Table == tbl.name {
				fk = key
			}
		}

		if fk == (Fk{}) {
			return "", "", err
		}
		sel += fmt.Sprintf("json_group_array(json_object(%s)) FILTER (WHERE [%s].[%s] IS NOT NULL) AS [%s], ", aggs, fk.Table, fk.From, tbl.name)

		joins += fmt.Sprintf("LEFT JOIN (%s) AS [%s] ON [%s].[%s] = [%s].[%s] ", query, tbl.name, fk.References, fk.To, fk.Table, fk.From)
	}

	return "SELECT " + sel[:len(sel)-2] + fmt.Sprintf(" FROM [%s] ", table.name) + joins, agg[:len(agg)-2], nil
}

func (schema SchemaCache) buildSelCurr(table Table, joinedOn string) (string, string, error) {
	var sel string
	var joins string
	var agg string
	includesFk := false
	var fk Fk

	if table.columns == nil && table.joins == nil {
		table.columns = []column{{"*", ""}}
	}

	if joinedOn != "" {
		for _, key := range schema.Fks {
			if key.References == joinedOn && key.Table == table.name {
				fk = key
			}
		}
	}

	for _, col := range table.columns {
		if joinedOn != "" && fk.Table == table.name && fk.From == col.name {
			includesFk = true
		}

		if col.name == "*" {
			sel += "*, "
			for name := range schema.Tables[table.name] {
				agg += fmt.Sprintf("'%s', [%s].[%s], ", name, table.name, name)
			}

			continue
		}

		sel += fmt.Sprintf("[%s].[%s], ", table.name, col.name)
		if col.alias != "" {
			agg += fmt.Sprintf("'%s', [%s].[%s], ", col.alias, table.name, col.name)
		} else {
			agg += fmt.Sprintf("'%s', [%s].[%s], ", col.name, table.name, col.name)
		}
	}

	if !includesFk {
		sel += fmt.Sprintf("[%s].[%s], ", fk.Table, fk.From)
	}

	for _, tbl := range table.joins {
		agg += fmt.Sprintf("'%s', json([%s]), ", tbl.name, tbl.name)
		query, aggs, err := schema.buildSelCurr(*tbl, table.name)
		if err != nil {
			return "", "", err
		}
		var fk Fk
		for _, key := range schema.Fks {
			if key.References == table.name && key.Table == tbl.name {
				fk = key
			}
		}
		if fk == (Fk{}) {
			return "", "", fmt.Errorf("no relationship exists in the schema cache between %s and %s", table.name, tbl.name)
		}

		sel += fmt.Sprintf("json_group_array(json_object(%s)) FILTER (WHERE [%s].[%s] IS NOT NULL) AS [%s], ", aggs, fk.Table, fk.From, tbl.name)

		joins += fmt.Sprintf("LEFT JOIN (%s) AS [%s] ON [%s].[%s] = [%s].[%s] ", query, tbl.name, fk.References, fk.To, fk.Table, fk.From)

	}

	return "SELECT " + sel[:len(sel)-2] + fmt.Sprintf(" FROM [%s] ", table.name) + joins, agg[:len(agg)-2], nil
}

func (schema SchemaCache) parseSelect(param string, table string) (Table, error) {
	tbl := Table{table, nil, nil, nil}
	currTbl := &tbl
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
					return Table{}, InvalidTblErr(currStr)
				}
				currTbl = &Table{currStr, nil, nil, currTbl}
				currTbl.parent.joins = append(currTbl.parent.joins, currTbl)
				currStr = ""
				alias = ""
			case ')':
				if currStr != "" {
					if currStr != "*" && schema.Tables[currTbl.name][currStr] == "" {
						return Table{}, InvalidColErr(currStr, currTbl.name)
					}
					currTbl.columns = append(currTbl.columns, column{currStr, alias})
					currStr = ""
				}
				alias = ""
				currTbl = currTbl.parent
			case ':':
				alias = currStr
				currStr = ""
			case ',':
				if currStr != "" {
					if currStr != "*" && schema.Tables[currTbl.name][currStr] == "" {
						return Table{}, InvalidColErr(currStr, currTbl.name)
					}
					currTbl.columns = append(currTbl.columns, column{currStr, alias})
					alias = ""
					currStr = ""
				}
			default:
				currStr += string(v)
			}
		}
	}

	if currStr != "" {
		if currStr != "*" && schema.Tables[currTbl.name][currStr] == "" {
			return Table{}, InvalidColErr(currStr, currTbl.name)
		}
		currTbl.columns = append(currTbl.columns, column{currStr, alias})
	}

	return tbl, nil
}

func (schema SchemaCache) buildOrder(table, param string) (string, error) {
	if param == "" {
		return "", nil
	}

	query := "ORDER BY "

	orderBy := splitParenthesis(param, table)

	fmt.Println(orderBy)

	for _, param := range orderBy {
		query += fmt.Sprintf("[%s].[%s] ", param.table, param.column)

		if len(param.ops) != 0 && (param.ops[0] == "asc" || param.ops[0] == "desc") {
			query += param.ops[0] + " "
		}

		query += ", "
	}

	return query[:len(query)-2], nil
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

	if params["or"] != nil {

		hasWhere = true

		prms := splitParenthesis(params["or"][0][1:len(params["or"][0])-1], table)

		for _, param := range prms {
			if i != 0 {
				query += "OR "
			}

			query += fmt.Sprintf("[%s].[%s] ", param.table, param.column)
			for _, op := range param.ops {
				if mapOperator(op) != "" {
					query += mapOperator(op) + " "
				} else {
					query += "? "
					args = append(args, op)
				}
			}

			i++
		}

		delete(params, "or")
	}

	for name, val := range params {
		if name != "order" && name != "select" && name != "or" {
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

type Param struct {
	table  string
	column string
	ops    []string
}

func splitParenthesis(s string, table string) []Param {
	inQuotes := false
	var params []Param
	param := Param{table, "", nil}
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
			if param.column == "" {
				param.column = currStr
			} else {
				param.ops = append(param.ops, currStr)
			}
			params = append(params, param)
			param = Param{}
			currStr = ""
		} else if v == '.' && !inQuotes {
			if param.column == "" {
				param.table = currStr
			} else {
				param.ops = append(param.ops, currStr)
			}
			currStr = ""
		} else if v == ':' && !inQuotes {
			param.column = currStr
			currStr = ""
		} else {
			currStr += string(v)
		}
	}

	if currStr != "" {
		if param.column == "" {
			param.column = currStr
		} else {
			param.ops = append(param.ops, currStr)
		}
	}
	if param.column != "" {
		params = append(params, param)
	}

	return params
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
