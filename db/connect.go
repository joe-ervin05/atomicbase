package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

type DbHandler func(db Database, req *http.Request) ([]interface{}, error)

type Response struct {
	Data  []interface{} `json:"data"`
	Error interface{}   `json:"error"`
}

// for endpoints that only work with the primary database
func WithPrimary(handler DbHandler) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao, err := connPrimary()

		req.Body = http.MaxBytesReader(wr, req.Body, 1048576)
		if err != nil {
			respErr(wr, err)
			return
		}

		data, err := handler(dao, req)
		if err != nil {
			respErr(wr, err)
			return
		}

		resp := Response{data, nil}

		body, err := json.Marshal(&resp)
		if err != nil {
			respErr(wr, err)
			return
		}

		wr.Write(body)
		defer dao.client.Close()
		defer req.Body.Close()

	}
}

// for endpoints that can use either the primary or an external database
func WithDb(handler DbHandler) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao, err := connDb(req)

		req.Body = http.MaxBytesReader(wr, req.Body, 1048576)
		if err != nil {
			respErr(wr, err)
			return
		}

		data, err := handler(dao, req)
		if err != nil {
			respErr(wr, err)
			return
		}

		resp := Response{data, nil}

		body, err := json.Marshal(&resp)
		if err != nil {
			respErr(wr, err)
			return
		}

		wr.Write(body)
		defer dao.client.Close()
		defer req.Body.Close()

	}
}

func respErr(wr http.ResponseWriter, err error) {
	resp := Response{nil, err.Error()}
	body, _ := json.Marshal(&resp)
	wr.WriteHeader(http.StatusInternalServerError)
	wr.Write(body)
}

func connDb(req *http.Request) (Database, error) {
	dbName := req.Header.Get("DB-Name")

	dao, err := connPrimary()
	if err != nil {
		return Database{}, err
	}

	if dbName != "" {
		err = dao.connTurso(dbName)
		if err != nil {
			return Database{}, err
		}
	}

	return dao, nil

}

func connPrimary() (Database, error) {
	client, err := sql.Open("libsql", "file:atomicdata/primary.db")
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping()

	if err != nil {
		log.Fatal(err)
	}

	var schema SchemaCache

	data, err := os.ReadFile("atomicdata/schema.gob")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			dao := Database{client, SchemaCache{}, 0}

			err = dao.InvalidateSchema()

			return dao, err
		}

		log.Fatal(err)
	}

	schema, err = loadSchema(data)

	return Database{client, schema, 0}, err
}

func (dao *Database) connTurso(dbName string) error {
	org := os.Getenv("TURSO_ORGANIZATION")

	if org == "" {
		return errors.New("TURSO_ORGANIZATION environment variable is not set but is required to access external databases")
	}

	id, token, schema, err := dao.QueryDbInfo(dbName)

	if err != nil {
		return err
	}
	// close the connection with the primary database
	dao.client.Close()

	client, err := sql.Open("libsql", fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", dbName, org, token))
	if err != nil {
		return err
	}

	err = client.Ping()

	if err != nil {
		return err
	}

	dao.id = id
	dao.client = client
	dao.Schema = schema

	return nil
}
