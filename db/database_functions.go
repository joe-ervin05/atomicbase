package db

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

// gets all turso dbs within an organization and stores them
func (dao Database) RegisterAllDbs(req *http.Request) error {
	org := os.Getenv("TURSO_ORGANIZATION")
	if org == "" {
		return errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
	}
	token := os.Getenv("TURSO_API_KEY")
	if token == "" {
		return errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
	}

	return nil
}

// creates a schema cache and stores it for an already existing turso db
func (dao Database) RegisterDb(req *http.Request) error {
	name := req.PathValue("name")
	dbToken := req.Header.Get("DB-Token")
	var err error

	if dbToken == "" {
		dbToken, err = createDbToken(name)
		if err != nil {
			fmt.Println("token error")
			return err
		}
	}

	org := os.Getenv("TURSO_ORGANIZATION")
	if org == "" {
		return errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
	}
	token := os.Getenv("TURSO_API_KEY")
	if token == "" {
		return errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
	}

	client := &http.Client{}
	request, err := http.NewRequest("GET", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s", org, name), nil)
	if err != nil {
		fmt.Println("retrieve error")
		return err
	}

	request.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(request)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		fmt.Println("retrieve error 2")
		return errors.New(res.Status)
	}

	newClient, err := sql.Open("libsql", fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", name, org, dbToken))
	if err != nil {
		return err
	}

	err = newClient.Ping()

	if err != nil {
		return err
	}

	cols, pks, err := schemaCols(newClient)
	if err != nil {
		return err
	}
	fks, err := schemaFks(newClient)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	schema := SchemaCache{cols, pks, fks}
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(schema)
	if err != nil {
		return err
	}

	_, err = dao.client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", name, dbToken, buf.Bytes())

	return err
}

func (dao Database) ListDbs(req *http.Request) ([]interface{}, error) {
	return dao.QueryMap("SELECT name, id from databases")
}

// for use with the primary database
func (dao Database) CreateDb(req *http.Request) error {

	group := req.URL.Query().Get("group")
	name := req.PathValue("name")

	if group == "" {
		group = "default"
	}

	type body struct {
		Name  string `json:"name"`
		Group string `json:"group"`
	}

	bod := body{name, group}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(bod)
	if err != nil {
		log.Fatal(err)
	}

	org := os.Getenv("TURSO_ORGANIZATION")
	if org == "" {
		return errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
	}
	token := os.Getenv("TURSO_API_KEY")
	if token == "" {
		return errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
	}

	client := &http.Client{}
	request, err := http.NewRequest("POST", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases", org), &buf)
	if err != nil {
		return err
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(request)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}

	newToken, err := createDbToken(name)

	if err != nil {
		return err
	}

	buf.Reset()
	var schema SchemaCache
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(schema)
	if err != nil {
		return err
	}

	_, err = dao.client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", name, newToken, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// for use with the primary database
func (dao Database) DeleteDb(req *http.Request) error {
	name := req.PathValue("name")

	_, err := dao.client.Exec("DELETE FROM databases WHERE name = ?", name)
	if err != nil {
		return err
	}

	org := os.Getenv("TURSO_ORGANIZATION")
	if org == "" {
		return errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
	}
	token := os.Getenv("TURSO_API_KEY")
	if token == "" {
		return errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
	}

	client := &http.Client{}
	request, err := http.NewRequest("DELETE", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s", org, name), nil)
	if err != nil {
		return err
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(request)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}

	return nil

}

func createDbToken(dbName string) (string, error) {
	org := os.Getenv("TURSO_ORGANIZATION")
	if org == "" {
		return "", errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
	}
	token := os.Getenv("TURSO_API_KEY")
	if token == "" {
		return "", errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s/auth/tokens", org, dbName), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", errors.New(res.Status)
	}

	dec := json.NewDecoder(res.Body)
	dec.DisallowUnknownFields()

	type jwtBody struct {
		Jwt string `json:"jwt"`
	}

	var jwtBod jwtBody
	err = dec.Decode(&jwtBod)

	return jwtBod.Jwt, err
}
