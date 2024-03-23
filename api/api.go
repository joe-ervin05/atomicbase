package api

import (
	"fmt"

	"github.com/gofiber/fiber/v3"
	"github.com/joe-ervin05/atomicbase/db"
)

func Run(app *fiber.App) {
	apiApp := app.Group("/api", db.DbMiddleware())

	apiApp.Get("/:table", handleGetRows())

	apiApp.Post("/:table", handlePostRows())

	apiApp.Delete("/:table", handleDeleteRows())

	apiApp.Patch("/:table", handlePatchRows())

	apiApp.Post("/udf/:funcName", handlePostUdf())
}

func handleGetRows() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao := c.Locals("dao").(db.Database)

		query, args, err := db.SelectRows(c)
		if err != nil {
			return err
		}

		j, err := dao.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(j)
		return nil
	}
}

func handlePostRows() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao := c.Locals("dao").(db.Database)

		query, args, err := db.InsertRows(c)
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
	}
}

func handlePatchRows() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao := c.Locals("dao").(db.Database)

		query, args, err := db.UpdateRows(c)
		if err != nil {
			return err
		}

		jsn, err := dao.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	}
}

func handleDeleteRows() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao := c.Locals("dao").(db.Database)

		query, args, err := db.DeleteRows(c.Queries(), c.Params("table"))
		fmt.Println(query)
		if err != nil {
			return err
		}

		jsn, err := dao.QueryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	}
}

func handlePostUdf() fiber.Handler {
	return func(c fiber.Ctx) error {
		dao := c.Locals("dao").(db.Database)
		_ = dao

		// handle user defined function requestss

		return nil
	}
}
