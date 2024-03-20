package main

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v3"
)

func handleGetTable() fiber.Handler {
	return withDb(func(c fiber.Ctx, db database) error {
		query, args, err := buildGetTable(c.Queries(), c.Params("table"))
		fmt.Println(query)
		if err != nil {
			return err
		}

		j, err := db.queryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(j)
		return nil
	})
}

func handlePostTable() fiber.Handler {
	return withDb(func(c fiber.Ctx, db database) error {

		bodyBytes := c.Request().Body()
		upsert := c.Get("Prefer") == "resolution=merge-duplicates"

		query, args, err := buildPostTable(bodyBytes, c.Queries(), c.Params("table"), upsert)
		fmt.Println(query, args)
		if err != nil {
			return err
		}

		jsn, err := db.queryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	})
}

func handleDeleteTable() fiber.Handler {
	return withDb(func(c fiber.Ctx, db database) error {
		query, args, err := buildDeleteTable(c.Queries(), c.Params("table"))
		fmt.Println(query)
		if err != nil {
			return err
		}

		jsn, err := db.queryJson(query, args...)
		if err != nil {
			return err
		}

		c.Response().SetBody(jsn)
		return nil
	})
}

func handlePostUdf() fiber.Handler {
	return withDb(func(c fiber.Ctx, db database) error {

		type Body struct {
			Args []any `json:"args"`
		}

		var body Body

		bodyBytes := c.Request().Body()
		err := json.Unmarshal(bodyBytes, &body)
		if err != nil {
			return err
		}

		query := ""
		var params []any

		for _, v := range body.Args {
			query += " ?"
			params = append(params, v)
		}

		fun := c.Params("funcName")

		db.client.Exec(fmt.Sprintf("%s(%s)", fun, query), params...)

		return nil
	})
}
