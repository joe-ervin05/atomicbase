package main

import (
	"log"

	"github.com/gofiber/fiber/v3"
	"github.com/joe-ervin05/atomicbase/api"
	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
}

func main() {
	app := fiber.New()

	api.Run(app)

	log.Fatal(app.Listen(":3000"))

}
