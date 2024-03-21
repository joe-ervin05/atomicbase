package db

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

var lock sync.Mutex

type SchemaCache struct{}

func saveSchema(data SchemaCache) error {
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

func loadSchema() (SchemaCache, error) {

	fData, err := os.ReadFile("schema_cache/schema.gob")
	if err != nil {
		return SchemaCache{}, err
	}
	buf := bytes.NewBuffer(fData)
	dec := gob.NewDecoder(buf)

	var s SchemaCache

	err = dec.Decode(&s)

	return s, err

}
