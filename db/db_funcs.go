package db

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// gets all turso dbs within an organization and stores them
// func (dao Database) RegisterAllDbs() error {
// 	type dbName struct {
// 		Name string
// 	}

// 	type databases struct {
// 		Databases []dbName `json:"databases"`
// 	}

// 	org := os.Getenv("TURSO_ORGANIZATION")
// 	if org == "" {
// 		return errors.New("TURSO_ORGANIZATION is not set but is required for managing turso databases")
// 	}
// 	token := os.Getenv("TURSO_API_KEY")
// 	if token == "" {
// 		return errors.New("TURSO_API_KEY is not set but is required for managing turso databases")
// 	}

// 	var client http.Client

// 	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases", org), nil)
// 	if err != nil {
// 		return err
// 	}
// 	req.Header.Set("Authorization", "Bearer "+token)

// 	res, err := client.Do(req)
// 	if err != nil {
// 		return err
// 	}
// 	if res.StatusCode != 200 {
// 		return errors.New(res.Status)
// 	}

// 	dec := json.NewDecoder(res.Body)

// 	var dbs databases

// 	err = dec.Decode(&dbs)
// 	if err != nil {
// 		return err
// 	}

// 	rows, err := dao.Client.Query("SELECT name FROM databases")
// 	if err != nil {
// 		return err
// 	}

// 	var currDbs []string

// 	for rows.Next() {
// 		var name sql.NullString

// 		rows.Scan(&name)
// 		currDbs = append(currDbs, name.String)
// 	}

// 	for _, db := range dbs.Databases {
// 		exists := false
// 		for i := 0; i < len(currDbs) && !exists; i++ {
// 			if db.Name == currDbs[i] {
// 				exists = true
// 			}
// 		}

// 		if !exists {
// 			dbToken, err := createDbToken(db.Name)
// 			if err != nil {
// 				return err
// 			}

// 			newClient, err := sql.Open("libsql", fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", db.Name, org, dbToken))
// 			if err != nil {
// 				return err
// 			}
// 			defer newClient.Close()

// 			err = newClient.Ping()

// 			if err != nil {
// 				return err
// 			}

// 			cols, pks, err := schemaCols(newClient)
// 			if err != nil {
// 				return err
// 			}
// 			fks, err := schemaFks(newClient)
// 			if err != nil {
// 				return err
// 			}

// 			var buf bytes.Buffer
// 			schema := SchemaCache{cols, pks, fks}
// 			enc := gob.NewEncoder(&buf)

// 			err = enc.Encode(schema)
// 			if err != nil {
// 				return err
// 			}

// 			_, err = dao.Client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", db.Name, dbToken, buf.Bytes())
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	return nil
// }

// creates a schema cache and stores it for an already existing turso db
func (dao Database) RegisterDb(body io.ReadCloser, dbToken string) error {
	type reqBody struct {
		Name string `json:"name"`
	}

	var bod reqBody

	json.NewDecoder(body).Decode(&bod)

	var err error

	if dbToken == "" {
		dbToken, err = createDbToken(bod.Name)
		if err != nil {
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
	request, err := http.NewRequest("GET", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s", org, bod.Name), nil)
	if err != nil {
		return err
	}

	request.Header.Set("Authorization", "Bearer "+token)

	res, err := client.Do(request)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}

	newClient, err := sql.Open("libsql", fmt.Sprintf("libsql://%s-%s.turso.io?authToken=%s", bod.Name, org, dbToken))
	if err != nil {
		return err
	}
	defer newClient.Close()

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

	_, err = dao.Client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", bod.Name, dbToken, buf.Bytes())

	return err
}

func (dao Database) ListDbs() ([]byte, error) {
	row := dao.Client.QueryRow("SELECT json_group_array(json_object('name', name, 'id', id)) AS data FROM (SELECT name, id from databases ORDER BY id)")

	if row.Err() != nil {
		return nil, row.Err()
	}

	var res []byte

	err := row.Scan(&res)

	return res, err
}

// for use with the primary database
func (dao Database) CreateDb(body io.ReadCloser) error {
	type reqBody struct {
		Name  string `json:"name"`
		Group string `json:"group"`
	}

	var bod reqBody

	err := json.NewDecoder(body).Decode(&bod)
	if err != nil {
		return err
	}

	if bod.Group == "" {
		bod.Group = "default"
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(bod)
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

	buf.Reset()
	var schema SchemaCache
	enc := gob.NewEncoder(&buf)

	err = enc.Encode(schema)
	if err != nil {
		return err
	}

	newToken, err := createDbToken(bod.Name)
	if err != nil {
		return err
	}

	_, err = dao.Client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", bod.Name, newToken, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// for use with the primary database
func (dao Database) DeleteDb(name string) error {

	_, err := dao.Client.Exec("DELETE FROM databases WHERE name = ?", name)
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
	type jwtBody struct {
		Jwt string `json:"jwt"`
	}

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

	var jwtBod jwtBody
	err = dec.Decode(&jwtBod)

	return jwtBod.Jwt, err
}
