package api

import (
	"net/http"

	"github.com/joe-ervin05/atomicbase/db"
)

func Run(app *http.ServeMux) {

	app.HandleFunc("GET /api/{table}", handleGetRows())
	app.HandleFunc("POST /api/{table}", handlePostRows())
	app.HandleFunc("PATCH /api/{table}", handlePatchRows())
	app.HandleFunc("DELETE /api/{table}", handleDeleteRows())

	app.HandleFunc("POST /api/schema/invalidate", handleInvalidateSchema())
	app.HandleFunc("POST /api/schema/table/{name}", handleCreateTable())
	app.HandleFunc("PATCH /api/schema/table/{name}", handleAlterTable())
	app.HandleFunc("DELETE /api/schema/table/{name}", handleDropTable())
	app.HandleFunc("POST /api/schema/edit", handleEditSchema())

	app.HandleFunc("POST /api/db/{name}", handleCreateDb())
	app.HandleFunc("DELETE /api/db/{name}", handleDeleteDb())

	app.HandleFunc("/api/udf/{funcName}", handlePostUdf())
}

func handleGetRows() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		return dao.SelectRows(req)
	})
}

func handlePostRows() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		return dao.InsertRows(req)
	})
}

func handlePatchRows() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		return dao.UpdateRows(req)
	})
}

func handleDeleteRows() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		return dao.DeleteRows(req)
	})
}

func handleCreateDb() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.CreateDb(req)
		return nil, err
	})
}

func handleDeleteDb() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.DeleteDb(req)
		return nil, err
	})
}

func handleInvalidateSchema() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.InvalidateSchema()
		return nil, err
	})
}

func handleEditSchema() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.EditSchema(req)
		return nil, err
	})
}

func handleCreateTable() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.CreateTable(req)
		return nil, err
	})
}

func handleAlterTable() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.AlterTable(req)
		return nil, err
	})
}

func handleDropTable() http.HandlerFunc {
	return db.WithDb(func(dao *db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.DropTable(req)
		return nil, err
	})
}

func handlePostUdf() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {

	}
}
