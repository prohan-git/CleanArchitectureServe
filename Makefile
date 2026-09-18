.PHONY: help build test lint run-server run-worker clean

GO  ?= go
BIN := bin

help:
	@echo "build        编译 server 与 worker 到 $(BIN)/"
	@echo "test         运行全部测试（含并发正确性测试）"
	@echo "lint         gofmt 检查 + go vet"
	@echo "run-server   本地启动控制面"
	@echo "run-worker   本地启动数据面"
	@echo "clean        清除构建产物与本地数据"

build:
	$(GO) build -o $(BIN)/server ./cmd/server
	$(GO) build -o $(BIN)/worker ./cmd/worker

test:
	$(GO) test -race ./...

lint:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run: gofmt -w ." && exit 1)
	$(GO) vet ./...

run-server: build
	./$(BIN)/server

run-worker: build
	./$(BIN)/worker

clean:
	rm -rf $(BIN) data
