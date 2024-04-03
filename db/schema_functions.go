package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type Column struct {
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primaryKey"`
	References string `json:"references"`
	OnDelete   string `json:"onDelete"`
	OnUpdate   string `json:"onUpdate"`
}

func (dao *Database) InvalidateSchema() error {

	pks, err := schemaPks(dao.client)
	if err != nil {
		return err
	}
	fks, err := schemaFks(dao.client)
	if err != nil {
		return err
	}
	cols, err := schemaCols(dao.client)
	if err != nil {
		return err
	}

	dao.Schema = SchemaCache{cols, pks, fks}

	return dao.saveSchema()
}

func (dao Database) RenameTable(req *http.Request) error {
	name := req.PathValue("name")

	if dao.Schema.Tables[name] == nil {
		return InvalidTblErr(name)
	}

	return nil
}

func (dao Database) RenameColumns(req *http.Request) error {
	name := req.PathValue("name")

	if dao.Schema.Tables[name] == nil {
		return InvalidTblErr(name)
	}

	return nil
}

func (dao Database) AddColumns(req *http.Request) error {
	name := req.PathValue("name")

	if dao.Schema.Tables[name] == nil {
		return InvalidTblErr(name)
	}

	return nil
}

func (dao Database) DropColumns(req *http.Request) error {
	name := req.PathValue("name")

	if dao.Schema.Tables[name] == nil {
		return InvalidTblErr(name)
	}

	column := req.PathValue("column")

	if dao.Schema.Tables[name][column] == "" {
		return invalidColErr(column, name)
	}

	_, err := dao.client.Exec("ALTER TABLE [%s] DROP COLUMN [%s]", name, column)

	return err
}

func (dao Database) CreateTable(req *http.Request) error {
	name := req.PathValue("name")
	query := "CREATE TABLE [" + name + "] ("

	var cols map[string]Column

	err := json.NewDecoder(req.Body).Decode(&cols)
	if err != nil {
		return err
	}

	type fKey struct {
		toTbl string
		toCol string
		col   string
	}

	var fKeys []fKey

	for n, col := range cols {

		query += fmt.Sprintf("[%s] %s", n, col.Type)
		if col.PrimaryKey {
			query += " PRIMARY KEY"
		}
		if col.References != "" {
			quoted := false
			fk := fKey{"", "", n}
			for i := 0; fk.toTbl == "" && i < len(col.References); i++ {
				if col.References[i] == '\'' {
					quoted = !quoted
				}
				if col.References[i] == '.' && !quoted {
					fk.toTbl = col.References[:i]
					fk.toCol = col.References[i+1:]
				}
			}
			fKeys = append(fKeys, fk)
		}

		query += ", "
	}

	for _, val := range fKeys {
		query += fmt.Sprintf("FOREIGN KEY([%s]) REFERENCES [%s]([%s]) ", val.col, val.toTbl, val.toCol)
		if cols[val.col].OnDelete != "" {
			query += "ON DELETE " + mapOnAction(cols[val.col].OnDelete) + " "
		}
		if cols[val.col].OnUpdate != "" {
			query += "ON UPDATE " + mapOnAction(cols[val.col].OnUpdate) + " "
		}
		query += ", "

	}

	query = query[:len(query)-2] + ")"

	_, err = dao.client.Exec(query)

	return err
}

func (dao Database) DropTable(req *http.Request) error {
	name := req.PathValue("name")

	if dao.Schema.Tables[name] == nil {
		return InvalidTblErr(name)
	}

	_, err := dao.client.Exec("DROP TABLE " + name)
	if err != nil {
		return err
	}

	return dao.InvalidateSchema()
}

func (dao Database) EditSchema(req *http.Request) error {
	type body struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}

	var bod body

	err := json.NewDecoder(req.Body).Decode(&bod)
	if err != nil {
		return err
	}

	_, err = dao.client.Exec(bod.Query, bod.Args...)
	if err != nil {
		return err
	}

	return dao.InvalidateSchema()
}

// map functions guarantee the input is an expected expression
// to limit vulnerabilities and prevent unexpected query affects

func mapColType(str string) string {
	switch strings.ToLower(str) {
	case "text":
		return "TEXT"
	case "integer":
		return "INTEGER"
	case "real":
		return "REAL"
	case "blob":
		return "BLOB"
	default:
		return ""
	}
}

func mapOnAction(str string) string {
	switch strings.ToLower(str) {
	case "no action":
		return "NO ACTION"
	case "restrict":
		return "RESTRICT"
	case "set null":
		return "SET NULL"
	case "set default":
		return "SET DEFAULT"
	case "cascade":
		return "CASCADE"
	default:
		return ""
	}
}
