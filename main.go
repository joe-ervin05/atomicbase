package main

import (
	"log"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
}

func main() {

	app := fiber.New()

	app.Get("/v1/:table", handleGetTable())

	app.Post("/v1/:table", handlePostTable())

	app.Delete("/v1/:table", handleDeleteTable())

	app.Post("/v1/udf/:funcName", handlePostUdf())

	log.Fatal(app.Listen(":3000"))
}
