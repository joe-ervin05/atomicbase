package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type relation struct {
	forKey   string
	ref      string
	operator string
}

type relationMap map[string]relation

func buildDeleteTable(params map[string]string, table string) (string, []any, error) {

	query := "DELETE FROM " + table + " "

	where, args, err := buildWhere(params)
	if err != nil {
		return "", nil, err
	}

	if where == "" {
		return "", nil, errors.New("the requested query would delete all rows. If this is intended please add a parameter like 1 = 1 for safety reasons")
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

func buildPostTable(body []byte, params map[string]string, table string, upsert bool) (string, []any, error) {
	query := ""
	var args []any

	if upsert {
		type columnSlice []map[string]any
		var cols columnSlice

		err := json.Unmarshal(body, &cols)
		if err != nil {
			return "", nil, err
		}

		insert, insArgs, err := buildUpsert(cols, table)
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

func buildReturning(param string) (string, error) {

	query := "RETURNING "

	keys := splitNotDelim(param, ',')

	for i, key := range keys {
		err := isSanitized(key)
		if err != nil {
			return "", err
		}

		if i == len(keys)-1 {
			query += key + " "
		} else {
			query += key + ", "
		}
	}

	return query, nil
}

func buildGetTable(params map[string]string, table string) (string, []any, error) {
	query := ""
	var args []any

	sel, err := buildSelect(params["select"], table)
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

func buildUpsert(colSlice []map[string]any, table string) (string, []any, error) {
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

	return query[:len(query)-2] + " ", args, nil

}

func buildInsert(cols map[string]any, table string) (string, []any, error) {

	query := "INSERT INTO " + table + " "
	args := make([]any, len(cols))

	i := 0
	columns := "( "
	values := "( "

	for col, val := range cols {
		i++
		args[i] = val
		columns += col + ", "
		values += "?, "
	}

	columns = columns[:len(columns)-2] + " ) "
	values = values[:len(values)-2] + ") "

	query += columns + "VALUES " + values

	return query, args, nil

}

func buildSelect(param string, table string) (string, error) {

	cols, rels, err := parseSelect(param)
	if err != nil {
		return "", err
	}

	query := "SELECT "

	for i, col := range cols {

		if i != len(cols)-1 {
			query += col + ", "
		} else {
			query += col + " "
		}

	}

	query += "FROM " + table + " "

	if len(rels) == 0 {
		return query, nil
	}

	// TODO add joins once cache is done

	// for tbl, rel := range rels {

	// 	query += fmt.Sprintf("LEFT JOIN %s ON %s %s %s ", tbl, rel.forKey, rel.operator, rel.ref)

	// }

	return query, nil
}

func parseSelect(str string) ([]string, map[string]string, error) {

	if str == "" {
		cols := make([]string, 1)
		cols[0] = "*"
		return cols, nil, nil
	}

	if strings.Count(str, "\"")%2 != 0 {
		return nil, nil, errors.New("the requested select query is not parsable because of unclosed quotation marks")
	}

	var relationMap map[string]string
	var cols []string
	inQuotes := false
	currTable := ""
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

			err := isSanitized(currStr)
			if err != nil {
				return cols, relationMap, err
			}

			prevTable = currTable
			currTable = currStr
			currStr = ""
		case ')':
			err := isSanitized(currStr)
			if err != nil {
				return cols, relationMap, err
			}

			cols = append(cols, dotSeparate(currTable, currStr))
			currTable = prevTable

		case ',':
			err := isSanitized(currStr)
			if err != nil {
				return cols, relationMap, err
			}

			cols = append(cols, dotSeparate(currTable, currStr))
			currStr = ""
		default:
			currStr += string(v)
		}
	}

	if currStr != "" {
		err := isSanitized(currStr)
		if err != nil {
			return cols, relationMap, err
		}
		cols = append(cols, dotSeparate(currTable, currStr))
	}

	return cols, relationMap, nil
}

func buildOrder(param string) (string, error) {
	if param == "" {
		return "", nil
	}

	query := "ORDER BY "

	orderBy := splitNotDelim(param, ',')

	for i, col := range orderBy {
		keys := splitNotDelim(col, '.')
		err := isSanitized(keys[0])
		if err != nil {
			return "", err
		}

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

		err := isSanitized(name)
		if err != nil {
			return "", nil, err
		}

		query += name + " "

		keys := splitNotDelim(val, '.')

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

func splitNotDelim(s string, delimiter rune) []string {
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
		"isnt":    "IS NOT",
		"and":     "AND",
		"or":      "OR",
	}

	return operators[str]
}

func isSanitized(param string) error {

	if param == "" {
		return errors.New("the requested url has an empty parameter")
	}

	inQuotes := false

	for _, v := range param {
		if v == '"' {
			inQuotes = !inQuotes
			continue
		}

		switch v {
		case ';':
			return charErr(";")
		case '-':
			return charErr("-")
		case '.':
			return charErr(".")
		}
	}

	return nil

}

func charErr(char string) error {
	return fmt.Errorf("the query you constructed contains a '%s' where it should not be. Use url encoded double quotes (%%22) if you need special characters in your value", char)
}

func dotSeparate(x, y string) string {
	if x == "" {
		return y
	}

	return x + "." + y
}
