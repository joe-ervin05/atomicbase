package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

type Database struct {
	client *sql.DB
	Schema SchemaCache
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

func initDb(req *http.Request) (Database, error) {
	dbName := req.Header.Get("DbName")
	if dbName == "" {
		dbName = os.Getenv("DB_NAME")
	}

	org := os.Getenv("TURSO_ORGANIZATION")

	token := req.Header.Get("Authorization")

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
		return Database{}, err
	}

	err = client.Ping()

	if err != nil {
		return Database{}, err
	}

	schema, err := loadSchema()
	if err != nil {
		pks, err := schemaPks(client)
		if err != nil {
			return Database{}, err
		}
		fks, err := schemaFks(client)
		if err != nil {
			return Database{}, err
		}
		cols, err := schemaCols(client)
		if err != nil {
			return Database{}, err
		}

		schema = SchemaCache{cols, pks, fks}

		err = saveSchema(schema)
		if err != nil {
			return Database{}, err
		}
	}

	return Database{client, schema}, nil
}

// runs a query and returns a json bytes encoding of the result
func (dao Database) QueryMap(query string, args ...any) ([]interface{}, error) {
	rows, err := dao.client.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
