package main

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

var lock sync.Mutex

func saveSchema(data schemaCache) error {
	// prevent concurrent writes
	lock.Lock()
	defer lock.Unlock()

	var buf bytes.Buffer

	err := os.Mkdir("schema_cache", os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create("schema_cache/schema.gob")
	if err != nil {
		return err
	}
	defer file.Close()

	enc := gob.NewEncoder(&buf)

	err = enc.Encode(data)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())

	return err
}

func loadSchema() (schemaCache, error) {

	fData, err := os.ReadFile("schema_cache/schema.gob")
	if err != nil {
		return schemaCache{}, err
	}
	buf := bytes.NewBuffer(fData)
	dec := gob.NewDecoder(buf)

	var s schemaCache

	err = dec.Decode(&s)

	return s, err

}
