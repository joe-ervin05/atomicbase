package api

import (
	"testing"

	"github.com/joe-ervin05/atomicbase/db"
)

func TestRequest(t *testing.T) {

	type tStruct struct {
		name   string
		field  string
		field2 string
	}

	ts := tStruct{"joe", "a", "b"}

	res, err := Request("GET", "http://localhost:8080/db", nil, ts)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	res, err = Request("GET", "http://localhost:8080/db", nil, nil)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()
}

func TestHandleCreateTable(t *testing.T) {

	type changes struct {
		Query string `json:"query"`
		Args  []any  `json:"args"`
	}

	ch := changes{`
	DROP TABLE IF EXISTS [users];
	DROP TABLE IF EXISTS [vehicles];
	DROP TABLE IF EXISTS [cars];
	DROP TABLE IF EXISTS [motorcycles];
	DROP TABLE IF EXISTS [tires];
	`, nil}

	res, err := Request("POST", "http://localhost:8080/schema", nil, ch)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	type users struct {
		Name     db.Column `json:"name"`
		Username db.Column `json:"username"`
		Id       db.Column `json:"id"`
	}

	name := db.Column{}
	name.Type = "TEXT"

	username := db.Column{}
	username.Type = "TEXT"
	username.Unique = true

	id := db.Column{}
	id.Type = "INTEGER"
	id.PrimaryKey = true

	tbl := users{name, username, id}

	res, err = Request("POST", "http://localhost:8080/schema/table/users", nil, tbl)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	type vehicles struct {
		Id     db.Column `json:"id"`
		UserId db.Column `json:"user_id"`
	}

	id = db.Column{}
	id.Type = "integer"
	id.PrimaryKey = true

	userId := db.Column{}
	userId.Type = "Integer"
	userId.References = "users.id"

	tbl2 := vehicles{id, userId}

	res, err = Request("POST", "http://localhost:8080/schema/table/vehicles", nil, tbl2)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	type cars struct {
		Id        db.Column `json:"id"`
		Test      db.Column `json:"test"`
		Test2     db.Column `json:"test2"`
		VehicleId db.Column `json:"vehicle_id"`
	}

	id = db.Column{}
	id.Type = "integer"
	id.PrimaryKey = true

	test := db.Column{}
	test.Type = "Text"

	test2 := db.Column{}
	test2.Type = "Integer"
	test2.NotNull = true

	vehicleId := db.Column{}
	vehicleId.Type = "Integer"
	vehicleId.References = "vehicles.id"

	tbl3 := cars{id, test, test2, vehicleId}

	res, err = Request("POST", "http://localhost:8080/schema/table/cars", nil, tbl3)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	type motorcycles struct {
		Id        db.Column `json:"id"`
		Brand     db.Column `json:"brand"`
		VehicleId db.Column `json:"vehicle_id"`
	}

	id = db.Column{}
	id.Type = "integer"
	id.PrimaryKey = true

	brand := db.Column{}
	brand.Type = "text"

	vehicleId = db.Column{}
	vehicleId.Type = "integer"
	vehicleId.References = "vehicles.id"

	tbl4 := motorcycles{id, brand, vehicleId}

	res, err = Request("POST", "http://localhost:8080/schema/table/motorcycles", nil, tbl4)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()

	type tires struct {
		Id    db.Column `json:"id"`
		Brand db.Column `json:"brand"`
		CarId db.Column `json:"car_id"`
	}

	id = db.Column{}
	id.Type = "integer"
	id.PrimaryKey = true

	carId := db.Column{}
	carId.Type = "integer"
	carId.References = "cars.id"

	tbl5 := tires{id, brand, carId}

	res, err = Request("POST", "http://localhost:8080/schema/table/tires", nil, tbl5)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()
}

func TestHandleAlterTable(t *testing.T) {
	dao, err := db.ConnPrimary()
	if err != nil {
		t.Error(err)
	}

	defer dao.Client.Close()

	// _, err = dao.Client.Exec(`
	// DROP TABLE IF EXISTS [_test_altertable];
	// CREATE TABLE IF NOT EXISTS [test_altertable] (
	// 	id INTEGER PRIMARY KEY,
	// 	name TEXT,
	// 	username TEXT UNIQUE
	// );`)
	// if err != nil {
	// 	t.Error(err)
	// }

	err = dao.InvalidateSchema()
	if err != nil {
		t.Error(err)
	}

	type tblChanges struct {
		NewName       string                  `json:"newName"`
		RenameColumns map[string]string       `json:"renameColumns"`
		NewColumns    map[string]db.NewColumn `json:"newColumns"`
		DropColums    []string                `json:"dropColumns"`
	}

	renameCols := map[string]string{"username": "test"}
	newCols := make(map[string]db.NewColumn)
	testCol := db.NewColumn{}
	testCol.Type = "real"
	testCol.NotNull = true
	testCol.Default = "0.1"
	newCols["test"] = testCol
	dropColumns := make([]string, 1)
	dropColumns[0] = "name"

	changes := tblChanges{"_test_newtable", renameCols, newCols, dropColumns}

	res, err := Request("PATCH", "http://localhost:8080/schema/table/test_altertable", nil, changes)
	if err != nil {
		t.Error(err)
	}

	res.Body.Close()
}
