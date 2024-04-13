package db

import (
	"testing"
)

func TestInvalidateSchema(t *testing.T) {
	dao, err := ConnPrimary()
	if err != nil {
		t.Error(err)
	}

	_, err = dao.Client.Exec(`
	CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY, 
		name TEXT, 
		img BLOB
	);
	CREATE TABLE IF NOT EXISTS cars (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		FOREIGN KEY(user_id) REFERENCES users(id)
	);`)
	if err != nil {
		t.Error(err)
	}

	err = dao.InvalidateSchema()
	if err != nil {
		t.Error(err)
	}

	if dao.Schema.Tables["cars"] == nil {
		t.Error("expected table cars in schema cache but table not found")
	}

	if dao.Schema.Tables["users"] == nil {
		t.Error("expected table users in schema cache but table not found")
	}

	if dao.Schema.Tables["users"]["img"] != "BLOB" {
		t.Error("expected column id on table users in schema cache not found")
	}

	if dao.Schema.Tables["cars"]["id"] != "INTEGER" {
		t.Error("expected column id on table users in schema cache not found")
	}

	if dao.Schema.Pks["cars"] != "id" {
		t.Error("expected primary key on cars to be id")
	}

	includesFk := false

	for _, fk := range dao.Schema.Fks {
		if fk.Table == "cars" && fk.From == "user_id" && fk.References == "users" && fk.To == "id" {
			includesFk = true
		}
	}

	if !includesFk {
		t.Error("expected foreign key from cars to users in schema cache")
	}

}
