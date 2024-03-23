package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	"github.com/gofiber/fiber/v3"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

type Database struct {
	Client *sql.DB
}

type SchemaCache struct {
	Tables TblMap
	Pks    PkMap
	Fks    []Fk
}

type Fk struct {
	Table      string
	References string
	From       string
	To         string
}

type TblMap map[string]map[string]string
type PkMap map[string]string

func DbMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao, cache, err := initDb(c)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}

		c.Locals("dao", dao)
		c.Locals("schema", cache)
		err = c.Next()

		return err
	}
}

func initDb(c fiber.Ctx) (Database, SchemaCache, error) {
	dbName := c.Get("DbName")
	if dbName == "" {
		dbName = os.Getenv("DB_NAME")
	}

	org := os.Getenv("TURSO_ORGANIZATION")

	token := c.Get("Authorization")

	var url string

	if dbName == "" {
		url = "file:primary.db"
	} else {
		authToken := token[7:]
		url = fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", dbName, org, authToken)
	}

	client, err := sql.Open("libsql", url)
	if err != nil {
		fmt.Println(err)
		return Database{}, SchemaCache{}, err
	}

	err = client.Ping()

	if err != nil {
		return Database{}, SchemaCache{}, err
	}

	schema, err := loadSchema()
	if err != nil {
		pks, err := schemaPks(client)
		if err != nil {
			return Database{}, SchemaCache{}, err
		}
		fks, err := schemaFks(client)
		if err != nil {
			return Database{}, SchemaCache{}, err
		}
		cols, err := schemaCols(client)
		if err != nil {
			return Database{}, SchemaCache{}, err
		}

		schema = SchemaCache{cols, pks, fks}

		err = saveSchema(schema)
		if err != nil {
			return Database{}, SchemaCache{}, err
		}
	}

	return Database{client}, schema, nil
}

// runs a query and returns a json bytes encoding of the result
func (dao Database) QueryMap(query string, args ...any) ([]interface{}, error) {
	rows, err := dao.Client.Query(query, args...)
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, err
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {

		scanArgs := make([]interface{}, count)

		for i, v := range columnTypes {

			// doesnt use scanType to support more sqlite drivers
			switch v.DatabaseTypeName() {
			case "TEXT":
				scanArgs[i] = new(sql.NullString)
			case "INTEGER":
				scanArgs[i] = new(sql.NullInt64)
			case "REAL":
				scanArgs[i] = new(sql.NullFloat64)
			case "BLOB":
				scanArgs[i] = new(sql.RawBytes)
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return nil, err
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}

			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	return finalRows, nil
}

func (dao Database) QueryJson(query string, args ...any) ([]byte, error) {
	jsn, err := dao.QueryMap(query, args...)
	if err != nil {
		return nil, err
	}

	return json.Marshal(jsn)

}

func schemaFks(db *sql.DB) ([]Fk, error) {

	var fks []Fk

	rows, err := db.Query(`
		SELECT m.name as "table", p."table" as "references", p."from", p."to"
		FROM sqlite_master m
		JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
		WHERE m.type = 'table'
		ORDER BY m.name;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var from, to, references, table sql.NullString

		rows.Scan(&table, &references, &from, &to)

		fks = append(fks, Fk{table.String, references.String, from.String, to.String})

	}

	return fks, err
}

func schemaCols(db *sql.DB) (TblMap, error) {

	tblMap := make(TblMap)

	rows, err := db.Query(`
		SELECT m.name, l.name as col, l.type as colType
		FROM sqlite_master m
		JOIN pragma_table_info(m.name) l
		WHERE m.type = 'table'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var col sql.NullString
		var colType sql.NullString
		var name sql.NullString

		rows.Scan(&name, &col, &colType)

		if tblMap[name.String] == nil {
			tblMap[name.String] = make(map[string]string)
		}
		tblMap[name.String][col.String] = colType.String
	}

	return tblMap, rows.Err()

}

func schemaPks(db *sql.DB) (map[string]string, error) {

	pkMap := make(map[string]string)

	rows, err := db.Query(`
		SELECT m.name, l.name as pk
		FROM sqlite_master m
		JOIN pragma_table_info(m.name) l ON l.pk = 1
		WHERE m.type = 'table'
		ORDER BY m.name;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var pk sql.NullString
		var name sql.NullString

		rows.Scan(&name, &pk)
		pkMap[name.String] = pk.String
	}

	return pkMap, rows.Err()

}
