.PHONY: gen-proto
gen-proto:
	protoc api/v1beta/ingestor_api.proto --go_out=paths=source_relative:. --go-grpc_out=require_unimplemented_servers=false:. --go-grpc_opt=paths=source_relative

.PHONY: gen-mocks
gen-mocks:
	go generate ./api/...

.PHONY: gen-all
gen-all: gen-proto gen-mocks