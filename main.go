package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/joe-ervin05/atomicbase/api"
	"github.com/joe-ervin05/atomicbase/db"
	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
}

func main() {
	app := http.NewServeMux()

	api.Run(app)

	server := http.Server{
		Addr:    ":8080",
		Handler: db.Middleware(app),
	}

	fmt.Println("Listening on port 8080")
	log.Fatal(server.ListenAndServe())

}
