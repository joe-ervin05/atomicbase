package db

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"log"
	"net/http"
)

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

	schema := SchemaCache{cols, pks, fks}

	if dao.id == 0 {
		err = saveSchema(schema)
		if err != nil {
			return err
		}
	} else {
		client, err := sql.Open("libsql", "file:atomicdata/primary.db")
		if err != nil {
			log.Fatal(err)
		}

		err = client.Ping()

		if err != nil {
			log.Fatal(err)
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		err = enc.Encode(schema)
		if err != nil {
			return err
		}

		_, err = client.Exec("UPDATE databases SET schema = ? WHERE id = ?", buf.Bytes(), dao.id)
		if err != nil {
			return err
		}
	}

	dao.Schema = schema

	return nil
}

func (dao *Database) editTable(req *http.Request) error {

	return nil
}

func (dao *Database) createTable(req *http.Request) error {

	return nil

}
