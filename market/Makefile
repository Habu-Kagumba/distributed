BINARY_NAME=market

all: deps build
install:
	go install .
build:
	go build .
test:
	go test -v ./...
clean:
	go clean
	rm -f $(BINARY_NAME)
deps:
	go build -v ./...
upgrade:
	go get -u
