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

type dbHandler func(fiber.Ctx, Database) error

type Database struct {
	Client *sql.DB
}

func WithDb(h dbHandler) fiber.Handler {
	return func(c fiber.Ctx) error {

		dbName := c.Get("DbName")
		if dbName == "" {
			dbName = os.Getenv("DB_NAME")
		}

		org := os.Getenv("TURSO_ORGANIZATION")

		token := c.Get("Authorization")

		dao, err := initDb(dbName, org, token)

		if err != nil {
			return c.Status(500).SendString(err.Error())
		}

		defer dao.Client.Close()

		return h(c, dao)
	}
}

func initDb(name, org, token string) (Database, error) {
	var url string

	if token == "" {
		url = "file:" + name
	} else {
		authToken := token[7:]
		url = fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", name, org, authToken)
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

	// scheme, err := loadSchema()
	// if err != nil {
	// 	_, err := querySchema(client)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return nil, err
	// 	}
	// 	saveSchema(scheme)
	// }

	// fmt.Println(scheme)

	return Database{client}, nil
}

// runs a query and returns a json bytes encoding of the result
func (dao Database) QueryJson(query string, args ...any) ([]byte, error) {
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

	return json.Marshal(finalRows)
}

// func querySchema(db *sql.DB) (schemaCache, error) {

// 	var scm schemaCache

// 	rows, err := db.Query(`SELECT 1.name FROM PRAGMA table_info("users") as 1 where 1.pk <> 0`)
// 	if err != nil {
// 		return scm, err
// 	}
// 	defer rows.Close()

// 	var sqls []int

// 	for rows.Next() {
// 		var sql int

// 		if err := rows.Scan(&sql); err != nil {
// 			return scm, err
// 		}

// 		fmt.Println(sql)
// 		sqls = append(sqls, sql)
// 	}

// 	return scm, rows.Err()
// }
