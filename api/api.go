package api

import (
	"net/http"

	"github.com/joe-ervin05/atomicbase/db"
)

func Run(app *http.ServeMux) {

	app.HandleFunc("GET /query/{table}", handleGetRows())
	app.HandleFunc("POST /query/{table}", handlePostRows())     // done
	app.HandleFunc("PATCH /query/{table}", handlePatchRows())   // done
	app.HandleFunc("DELETE /query/{table}", handleDeleteRows()) // done

	app.HandleFunc("POST /schema", handleEditSchema())                  // done
	app.HandleFunc("POST /schema/invalidate", handleInvalidateSchema()) // done

	app.HandleFunc("POST /schema/table/{name}", handleCreateTable()) // done
	app.HandleFunc("DELETE /schema/table/{name}", handleDropTable()) // done
	app.HandleFunc("PATCH /schema/table/{name}", handleAlterTable())

	app.HandleFunc("GET /db", handleListDbs())             // done
	app.HandleFunc("POST /db/{name}", handleCreateDb())    // done
	app.HandleFunc("PATCH /db/{name}", handleRegisterDb()) // done
	app.HandleFunc("PATCH /db", handleRegisterAll())       //
	app.HandleFunc("DELETE /db/{name}", handleDeleteDb())  // done

	app.HandleFunc("/udf/{funcName}", handlePostUdf())
}

func handleGetRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return dao.SelectRows(req)
	})
}

func handlePostRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return dao.InsertRows(req)
	})
}

func handlePatchRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return dao.UpdateRows(req)
	})
}

func handleDeleteRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return dao.DeleteRows(req)
	})
}

func handleCreateDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.CreateDb(req)
		return nil, err
	})
}

func handleRegisterAll() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.RegisterAllDbs()
		return nil, err

	})
}

func handleRegisterDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.RegisterDb(req)
		return nil, err
	})
}

func handleListDbs() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return dao.ListDbs()
	})
}

func handleDeleteDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]interface{}, error) {

		err := dao.DeleteDb(req)
		return nil, err
	})
}

func handleInvalidateSchema() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.InvalidateSchema()
		return nil, err
	})
}

func handleEditSchema() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.EditSchema(req)
		return nil, err
	})
}

func handleCreateTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.CreateTable(req)
		return nil, err
	})
}

func handleDropTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.DropTable(req)
		return nil, err
	})
}

func handleAlterTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		err := dao.RenameTable(req)
		return nil, err
	})
}

func handlePostUdf() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]interface{}, error) {
		return nil, nil
	})
}
