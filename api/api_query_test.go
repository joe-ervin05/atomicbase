package api

import (
	"encoding/json"
	"testing"
)

func TestHandleGetRows(t *testing.T) {

	TestHandleCreateTable(t)

	for i := 0; i < 100; i++ {

		res, err := Request("GET", "http://localhost:8080/query/users?select=*,vehicles(cars(*,tires(brand,id)),motorcycles(name:brand),id)", nil, nil)
		if err != nil {
			t.Error(err)
		}
		var bod any
		err = json.NewDecoder(res.Body).Decode(&bod)
		if err != nil {
			t.Error(err)
		}

		res.Body.Close()
	}

}

func TestHandleInsertRows(t *testing.T) {

}
