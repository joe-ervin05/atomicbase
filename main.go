package main

import (
	"github.com/joe-ervin05/atomicbase/api"
	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()
}

func main() {

	srvr := api.NewApiServer(":3000")

	srvr.Run()

}
