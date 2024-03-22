package db

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
)

var lock sync.Mutex

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
