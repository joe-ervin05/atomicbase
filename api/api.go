package api

import (
	"fmt"
	"log"

	"github.com/gofiber/fiber/v3"
	"github.com/joe-ervin05/atomicbase/db"
	"github.com/joe-ervin05/atomicbase/querybuilder"
)

type ApiServer struct {
	listen string
}

func NewApiServer(listenAddr string) *ApiServer {
	return &ApiServer{
		listen: listenAddr,
	}
}

func (srvr *ApiServer) Run() {
	app := fiber.New()

	app.Get("/v1/:table", srvr.handleGetRows())

	app.Post("/v1/:table", srvr.handlePostRows())

	app.Delete("/v1/:table", srvr.handleDeleteRows())

	app.Post("/v1/udf/:funcName", srvr.handlePostUdf())

	log.Fatal(app.Listen(srvr.listen))
}

func (srvr *ApiServer) handleGetRows() fiber.Handler {
	return db.WithDb(func(c fiber.Ctx, dao db.Database) error {
		query, args, err := querybuilder.SelectRows(c.Queries(), c.Params("table"))
		if err != nil {
			return err
		}

		j, err := dao.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(j)
		return nil
	})
}

func (srvr *ApiServer) handlePostRows() fiber.Handler {
	return db.WithDb(func(c fiber.Ctx, dao db.Database) error {

		bodyBytes := c.Request().Body()
		upsert := c.Get("Prefer") == "resolution=merge-duplicates"

		query, args, err := querybuilder.InsertRows(bodyBytes, c.Queries(), c.Params("table"), upsert)
		fmt.Println(query, args)
		if err != nil {
			return err
		}

		jsn, err := dao.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	})
}

func (srvr *ApiServer) handleDeleteRows() fiber.Handler {
	return db.WithDb(func(c fiber.Ctx, db db.Database) error {
		query, args, err := querybuilder.DeleteRows(c.Queries(), c.Params("table"))
		fmt.Println(query)
		if err != nil {
			return err
		}

		jsn, err := db.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	})
}

func (srvr *ApiServer) handlePostUdf() fiber.Handler {
	return db.WithDb(func(c fiber.Ctx, dao db.Database) error {

		fmt.Println(dao.Schema.Fks)

		return nil
	})
}
