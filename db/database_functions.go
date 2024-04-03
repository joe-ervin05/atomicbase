package db

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

// creates a schema cache and stores it for an already existing turso db
func (dao Database) RegisterDb(req *http.Request) error {
	return nil
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

	req, err = http.NewRequest("POST", fmt.Sprintf("https://api.turso.tech/v1/organizations/%s/databases/%s/auth/tokens", org, name), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err = client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}

	dec := json.NewDecoder(res.Body)
	dec.DisallowUnknownFields()

	type jwtBody struct {
		Jwt string `json:"jwt"`
	}

	var jwtBod jwtBody
	err = dec.Decode(&jwtBod)

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

	_, err = dao.client.Exec("INSERT INTO databases (name, token, schema) values (?, ?, ?)", name, jwtBod.Jwt, buf.Bytes())
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