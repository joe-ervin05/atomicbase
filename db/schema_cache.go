package db

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sync"
)

var lock sync.Mutex

// TODO join all the schema queries into one query with multiple joins

func querySchema(db *sql.DB) (SchemaCache, error) {
	var fks []Fk
	tblMap := make(TblMap)
	pkMap := make(map[string]string)

	rows, err := db.Query(`
		SELECT m.name as "table", f."table" as "references", f."from", f."to", l.name as col, l.type as colType, p.name as pk
		FROM sqlite_master m
		JOIN pragma_foreign_key_list(m.name) f ON m.name != f."table"
		JOIN pragma_table_info(m.name) l
		JOIN pragma_table_info(m.name) p ON p.pk = 1
		WHERE m.type = 'table'
		ORDER BY m.name;
	`)
	if err != nil {
		return SchemaCache{}, err
	}

	_ = rows

	return SchemaCache{tblMap, pkMap, fks}, nil

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

func schemaCols(db *sql.DB) (TblMap, map[string]string, error) {

	tblMap := make(TblMap)
	pkMap := make(map[string]string)

	rows, err := db.Query(`
		SELECT m.name, l.name as col, l.type as colType, l.pk
		FROM sqlite_master m
		JOIN pragma_table_info(m.name) l
		WHERE m.type = 'table'
	`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var col sql.NullString
		var colType sql.NullString
		var name sql.NullString
		var pk sql.NullBool

		rows.Scan(&name, &col, &colType, &pk)

		if tblMap[name.String] == nil {
			tblMap[name.String] = make(map[string]string)
		}
		tblMap[name.String][col.String] = colType.String
		if pk.Bool {
			pkMap[name.String] = col.String
		}
	}

	fmt.Println("test")
	fmt.Println(tblMap, pkMap)

	return tblMap, pkMap, rows.Err()

}

// func schemaPks(db *sql.DB) (map[string]string, error) {

// 	pkMap := make(map[string]string)

// 	rows, err := db.Query(`
// 		SELECT m.name, l.name as pk
// 		FROM sqlite_master m
// 		JOIN pragma_table_info(m.name) l ON l.pk = 1
// 		WHERE m.type = 'table'
// 		ORDER BY m.name;
// 	`)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var pk sql.NullString
// 		var name sql.NullString

// 		rows.Scan(&name, &pk)
// 		pkMap[name.String] = pk.String
// 	}

// 	return pkMap, rows.Err()

// }

func (dao Database) saveSchema() error {
	if dao.id == 0 {
		// prevent concurrent writes
		lock.Lock()
		defer lock.Unlock()

		var buf bytes.Buffer

		file, err := os.Create("atomicdata/schema.gob")
		if err != nil {
			return err
		}
		defer file.Close()

		enc := gob.NewEncoder(&buf)

		err = enc.Encode(dao.Schema)
		if err != nil {
			return err
		}

		_, err = file.Write(buf.Bytes())

		return err
	}

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

	err = enc.Encode(dao.Schema)
	if err != nil {
		return err
	}

	_, err = client.Exec("UPDATE databases SET schema = ? WHERE id = ?", buf.Bytes(), dao.id)

	return err
}

func loadSchema(data []byte) (SchemaCache, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var schema SchemaCache

	err := dec.Decode(&schema)

	return schema, err

}
