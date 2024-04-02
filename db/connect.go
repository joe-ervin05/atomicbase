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

type DbHandler func(db *Database, req *http.Request) ([]interface{}, error)

func WithDb(handler DbHandler) http.HandlerFunc {

	return func(wr http.ResponseWriter, req *http.Request) {
		type Response struct {
			Data  []interface{} `json:"data"`
			Error interface{}   `json:"error"`
		}

		req.Body = http.MaxBytesReader(wr, req.Body, 1048576)
		dao, err := connDb(req)
		if err != nil {
			resp := Response{nil, err.Error()}
			body, _ := json.Marshal(&resp)
			wr.WriteHeader(http.StatusInternalServerError)
			wr.Write(body)
			return
		}

		data, err := handler(dao, req)
		if err != nil {
			resp := Response{nil, err.Error()}
			body, _ := json.Marshal(&resp)
			wr.WriteHeader(http.StatusInternalServerError)
			wr.Write(body)
			return
		}

		resp := Response{data, nil}

		body, err := json.Marshal(&resp)
		if err != nil {
			resp := Response{nil, err.Error()}
			body, _ := json.Marshal(&resp)
			wr.WriteHeader(http.StatusInternalServerError)
			wr.Write(body)
			return
		}
		defer req.Body.Close()

		wr.Write(body)
	}
}

func connDb(req *http.Request) (*Database, error) {
	dbName := req.Header.Get("DB-Name")

	dao, err := connPrimary()
	if err != nil {
		return &Database{}, err
	}

	if dbName == "" {
		return dao, nil
	}

	err = dao.connTurso(dbName)
	if err != nil {
		return &Database{}, err
	}

	return dao, nil

}

func connPrimary() (*Database, error) {
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
		pks, err := schemaPks(client)
		if err != nil {
			log.Fatal(err)
		}
		fks, err := schemaFks(client)
		if err != nil {
			log.Fatal(err)
		}
		cols, err := schemaCols(client)
		if err != nil {
			log.Fatal(err)
		}

		schema = SchemaCache{cols, pks, fks}

		err = saveSchema(schema)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		schema, err = loadSchema(data)
		if err != nil {
			return &Database{}, err
		}
	}

	return &Database{client, schema, 0}, nil
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
