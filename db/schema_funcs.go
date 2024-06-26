package db

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type Column struct {
	Type       string `json:"type"`
	Default    any    `json:"default"`
	PrimaryKey bool   `json:"primaryKey"`
	Unique     bool   `json:"unique"`
	NotNull    bool   `json:"notNull"`
	References string `json:"references"`
	OnDelete   string `json:"onDelete"`
	OnUpdate   string `json:"onUpdate"`
}

type NewColumn struct {
	Type       string `json:"type"`
	Default    any    `json:"default"`
	NotNull    bool   `json:"notNull"`
	References string `json:"references"`
	OnDelete   string `json:"onDelete"`
	OnUpdate   string `json:"onUpdate"`
}

func (dao *Database) InvalidateSchema() error {

	cols, pks, err := schemaCols(dao.Client)
	if err != nil {
		return err
	}
	fks, err := schemaFks(dao.Client)
	if err != nil {
		return err
	}

	dao.Schema = SchemaCache{cols, pks, fks}

	return dao.saveSchema()
}

func (dao Database) AlterTable(table string, body io.ReadCloser) error {
	type tblChanges struct {
		NewName       string               `json:"newName"`
		RenameColumns map[string]string    `json:"renameColumns"`
		NewColumns    map[string]NewColumn `json:"newColumns"`
		DropColums    []string             `json:"dropColumns"`
	}

	if dao.Schema.Tables[table] == nil {
		return InvalidTblErr(table)
	}

	query := ""

	var changes tblChanges
	err := json.NewDecoder(body).Decode(&changes)
	if err != nil {
		return err
	}

	if changes.RenameColumns != nil {
		for col, new := range changes.RenameColumns {
			if dao.Schema.Tables[table][col] == "" {
				return InvalidColErr(col, table)
			}

			query += fmt.Sprintf("ALTER TABLE ["+table+"] RENAME COLUMN [%s] TO [%s]; ", col, new)
		}
	}

	if changes.DropColums != nil {
		for _, col := range changes.DropColums {
			if dao.Schema.Tables[table][col] == "" {
				return InvalidColErr(col, table)
			}

			query += fmt.Sprintf("ALTER TABLE ["+table+"] DROP COLUMN [%s]; ", col)
		}
	}

	if changes.NewColumns != nil {
		for name, col := range changes.NewColumns {
			if mapColType(col.Type) == "" {
				return InvalidTypeErr(name, col.Type)
			}

			query += fmt.Sprintf("ALTER TABLE ["+table+"] ADD COLUMN [%s] %s ", name, mapColType(col.Type))

			if col.NotNull {
				query += "NOT NULL "
			}
			if col.Default != nil {
				switch col.Default.(type) {
				case string:
					query += fmt.Sprintf(`DEFAULT "%s" `, col.Default)
				case float64:
					query += fmt.Sprintf("DEFAULT %g ", col.Default)
				}
			}

			if col.References != "" {
				quoted := false
				toTbl := ""
				toCol := ""
				for i := 0; toTbl == "" && i < len(col.References); i++ {
					if col.References[i] == '\'' {
						quoted = !quoted
					}
					if col.References[i] == '.' && !quoted {
						toTbl = col.References[:i]
						if dao.Schema.Tables[toTbl] == nil {
							return InvalidTblErr(toTbl)
						}
						toCol = col.References[i+1:]
						if dao.Schema.Tables[toTbl][toCol] == "" {
							return InvalidColErr(toCol, toTbl)
						}
					}
				}

				query += fmt.Sprintf("REFERENCES [%s]([%s]) ", toTbl, toCol)
				if col.OnDelete != "" {
					query += "ON DELETE " + mapOnAction(col.OnDelete) + " "
				}
				if col.OnUpdate != "" {
					query += "ON UPDATE " + mapOnAction(col.OnUpdate) + " "
				}
			}

			query += "; "
		}
	}

	if changes.NewName != "" {
		query += "ALTER TABLE [" + table + "] RENAME TO [" + changes.NewName + "]; "
	}

	fmt.Println(query)

	_, err = dao.Client.Exec(query)
	if err != nil {
		return err
	}

	return dao.InvalidateSchema()
}

func (dao Database) CreateTable(table string, body io.ReadCloser) error {
	query := "CREATE TABLE [" + table + "] ("

	var cols map[string]Column

	err := json.NewDecoder(body).Decode(&cols)
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
		if mapColType(col.Type) == "" {
			return InvalidTypeErr(n, col.Type)
		}

		query += fmt.Sprintf("[%s] %s ", n, mapColType(col.Type))
		if col.PrimaryKey {
			query += "PRIMARY KEY "
		}
		if col.Unique {
			query += "UNIQUE "
		}
		if col.NotNull {
			query += "NOT NULL "
		}
		if col.Default != nil {
			switch col.Default.(type) {
			case string:
				query += fmt.Sprintf(`DEFAULT "%s" `, col.Default)
			case float64:
				query += fmt.Sprintf("DEFAULT %g ", col.Default)
			}
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
					if dao.Schema.Tables[fk.toTbl] == nil {
						return InvalidTblErr(fk.toTbl)
					}
					fk.toCol = col.References[i+1:]
					if dao.Schema.Tables[fk.toTbl][fk.toCol] == "" {
						return InvalidColErr(fk.toCol, fk.toTbl)
					}
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

	_, err = dao.Client.Exec(query)
	if err != nil {
		return err
	}

	return dao.InvalidateSchema()
}

func (dao Database) DropTable(table string) error {

	if dao.Schema.Tables[table] == nil {
		return InvalidTblErr(table)
	}

	_, err := dao.Client.Exec("DROP TABLE " + table)
	if err != nil {
		return err
	}

	return dao.InvalidateSchema()
}

func (dao Database) EditSchema(body io.ReadCloser) error {
	type reqBody struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}

	var bod reqBody

	err := json.NewDecoder(body).Decode(&bod)
	if err != nil {
		return err
	}

	_, err = dao.Client.Exec(bod.Query, bod.Args...)
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
