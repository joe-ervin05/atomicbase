package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/joe-ervin05/atomicbase/db"
)

func Run(app *http.ServeMux) {

	app.HandleFunc("GET /query/{table}", handleSelectRows())    // done
	app.HandleFunc("POST /query/{table}", handleInsertRows())   // done
	app.HandleFunc("PATCH /query/{table}", handleUpdateRows())  // done
	app.HandleFunc("DELETE /query/{table}", handleDeleteRows()) // done

	app.HandleFunc("POST /schema", handleEditSchema())                  // done
	app.HandleFunc("POST /schema/invalidate", handleInvalidateSchema()) // done

	app.HandleFunc("POST /schema/table/{table}", handleCreateTable()) // done
	app.HandleFunc("DELETE /schema/table/{table}", handleDropTable()) // done
	app.HandleFunc("PATCH /schema/table/{table}", handleAlterTable()) // done

	app.HandleFunc("GET /db", handleListDbs())            // done
	app.HandleFunc("POST /db", handleCreateDb())          // done
	app.HandleFunc("PATCH /db", handleRegisterDb())       // done
	app.HandleFunc("DELETE /db/{name}", handleDeleteDb()) // done

	app.HandleFunc("/udf/{funcName}", handlePostUdf())
}

func handleSelectRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {

		return dao.SelectRows(req.PathValue("table"), req.URL.Query())
	})
}

func handleInsertRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		upsert := req.Header.Get("Prefer") == "resolution=merge-duplicates"

		return dao.InsertRows(req.PathValue("table"), req.URL.Query(), req.Body, upsert)
	})
}

func handleUpdateRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {

		return dao.UpdateRows(req.PathValue("table"), req.URL.Query(), req.Body)
	})
}

func handleDeleteRows() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {

		return dao.DeleteRows(req.PathValue("table"), req.URL.Query())
	})
}

func handleCreateDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]byte, error) {

		err := dao.CreateDb(req.Body)
		return nil, err
	})
}

// func handleRegisterAll() http.HandlerFunc {
// 	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]byte, error) {

// 		err := dao.RegisterAllDbs()
// 		return nil, err

// 	})
// }

func handleRegisterDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]byte, error) {

		err := dao.RegisterDb(req.Body, req.Header.Get("DB-Token"))
		return nil, err
	})
}

func handleListDbs() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]byte, error) {
		return dao.ListDbs()
	})
}

func handleDeleteDb() http.HandlerFunc {
	return db.WithPrimary(func(dao db.Database, req *http.Request) ([]byte, error) {

		err := dao.DeleteDb(req.PathValue("name"))
		return nil, err
	})
}

func handleInvalidateSchema() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		err := dao.InvalidateSchema()
		return nil, err
	})
}

func handleEditSchema() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		err := dao.EditSchema(req.Body)
		return nil, err
	})
}

func handleCreateTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		err := dao.CreateTable(req.PathValue("table"), req.Body)
		return nil, err
	})
}

func handleDropTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		err := dao.DropTable(req.PathValue("table"))
		return nil, err
	})
}

func handleAlterTable() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		err := dao.AlterTable(req.PathValue("table"), req.Body)
		return nil, err
	})
}

func handlePostUdf() http.HandlerFunc {
	return db.WithDb(func(dao db.Database, req *http.Request) ([]byte, error) {
		return nil, nil
	})
}

func Request(method, url string, headers map[string]string, body any) (*http.Response, error) {
	client := &http.Client{}
	var req *http.Request
	var err error

	if body != nil {
		var buf bytes.Buffer

		err = json.NewEncoder(&buf).Encode(body)
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest(method, url, &buf)
		if err != nil {
			return nil, err
		}
	} else {
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
		}
	}

	for name, val := range headers {
		req.Header.Add(name, val)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		bod, err := io.ReadAll(res.Body)
		if err != nil {
			return res, err
		}

		if bod == nil {
			return res, errors.New(res.Status)
		}
		return res, errors.New(string(bod))
	}

	return res, nil
}
