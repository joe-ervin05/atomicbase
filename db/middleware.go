package db

import (
	"context"
	"net/http"
)

type dbKey string

var Key dbKey = "db.middleware.dao"

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		// ensures the body of the request is no larger than 1 mb
		req.Body = http.MaxBytesReader(wr, req.Body, 1048576)
		dao, err := initDb(req)
		if err != nil {
			wr.WriteHeader(http.StatusInternalServerError)
			wr.Write([]byte(err.Error()))
		}

		ctx := context.WithValue(req.Context(), Key, dao)
		request := req.WithContext(ctx)

		next.ServeHTTP(wr, request)
	})
}
