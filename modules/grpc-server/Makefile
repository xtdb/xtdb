protoc:
	protoc --clojure_out=grpc-client,grpc-server:src --proto_path=resources resources/utils.proto resources/entity.proto resources/transaction.proto resources/service.proto

setup:
	sudo curl -L https://github.com/protojure/protoc-plugin/releases/download/v2.0.0/protoc-gen-clojure --output /usr/local/bin/protoc-gen-clojure
	sudo chmod +x /usr/local/bin/protoc-gen-clojure