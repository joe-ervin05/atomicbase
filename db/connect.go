package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

type DbHandler func(db Database, req *http.Request) ([]byte, error)

type Response struct {
	Data  []byte      `json:"data"`
	Error interface{} `json:"error"`
}

// for endpoints that only work with the primary database
func WithPrimary(handler DbHandler) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao, err := ConnPrimary()

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

		wr.Write(data)
		defer dao.Client.Close()
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

		wr.Write(data)
		defer dao.Client.Close()
		defer req.Body.Close()

	}
}

func respErr(wr http.ResponseWriter, err error) {
	wr.WriteHeader(http.StatusInternalServerError)
	wr.Write([]byte(err.Error()))
}

func connDb(req *http.Request) (Database, error) {
	dbName := req.Header.Get("DB-Name")

	dao, err := ConnPrimary()
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

func ConnPrimary() (Database, error) {

	client, err := sql.Open("libsql", "file:atomicdata/primary.db")
	if err != nil {
		return Database{}, err
	}

	schema, err := QueryPrimaryInfo(client)

	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping()

	if err != nil {
		log.Fatal(err)
	}

	return Database{client, schema, 1}, err
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
	dao.Client.Close()

	client, err := sql.Open("libsql", fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", dbName, org, token))
	if err != nil {
		return err
	}

	err = client.Ping()

	if err != nil {
		return err
	}

	dao.id = id
	dao.Client = client
	dao.Schema = schema

	return nil
}
