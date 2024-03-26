package api

import (
	"io"
	"net/http"

	"github.com/joe-ervin05/atomicbase/db"
)

func Run(app *http.ServeMux) {

	app.HandleFunc("GET /api/{table}", handleGetRows())
	app.HandleFunc("POST /api/{table}", handlePostRows())
	app.HandleFunc("PATCH /api/{table}", handlePatchRows())
	app.HandleFunc("DELETE /api/{table}", handleDeleteRows())

	app.HandleFunc("/api/udf/{funcName}", handlePostUdf())
}

func handleGetRows() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao := req.Context().Value(db.Key).(db.Database)
		res, err := dao.SelectRows(req)
		// drain body
		io.Copy(io.Discard, req.Body)
		defer req.Body.Close()
		if err != nil {
			handleErr(wr, err)
			return
		}

		wr.Write(res)
	}
}

func handlePostRows() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao := req.Context().Value(db.Key).(db.Database)
		res, err := dao.InsertRows(req)
		defer req.Body.Close()
		if err != nil {
			handleErr(wr, err)
			return
		}

		wr.Write(res)
	}
}

func handlePatchRows() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao := req.Context().Value(db.Key).(db.Database)
		res, err := dao.UpdateRows(req)
		defer req.Body.Close()
		if err != nil {
			handleErr(wr, err)
			return
		}

		wr.Write(res)
	}
}

func handleDeleteRows() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		dao := req.Context().Value(db.Key).(db.Database)
		res, err := dao.DeleteRows(req)
		io.Copy(io.Discard, req.Body)
		defer req.Body.Close()
		if err != nil {
			handleErr(wr, err)
			return
		}

		wr.Write(res)
	}
}

func handlePostUdf() http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {

	}
}

func handleErr(wr http.ResponseWriter, err error) {
	wr.WriteHeader(http.StatusInternalServerError)
	wr.Write([]byte(err.Error()))
}
