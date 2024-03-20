
build:
	@go build -o bin/restapi

run: build
	@./bin/restapi

test:
	@go test -v ./...