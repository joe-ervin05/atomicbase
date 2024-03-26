package db

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"os"
	"sync"
)

var lock sync.Mutex

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

func saveSchema(schema SchemaCache) error {
	// prevent concurrent writes
	lock.Lock()
	defer lock.Unlock()

	var buf bytes.Buffer

	err := os.MkdirAll("atomicdata", os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create("atomicdata/schema.gob")
	if err != nil {
		return err
	}
	defer file.Close()

	enc := gob.NewEncoder(&buf)

	err = enc.Encode(schema)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())

	return err
}

func loadSchema() (SchemaCache, error) {

	fData, err := os.ReadFile("atomicdata/schema.gob")
	if err != nil {
		return SchemaCache{}, err
	}
	buf := bytes.NewBuffer(fData)
	dec := gob.NewDecoder(buf)

	var schema SchemaCache

	err = dec.Decode(&schema)

	return schema, err

}
