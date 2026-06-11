#!/bin/sh
# Генерация Go- и TS-кода из proto-схем. Запуск из корня репозитория:
#   sh proto/gen.sh
set -eu

cd "$(dirname "$0")/.."

protoc \
	--proto_path=proto \
	--go_out=. \
	--go_opt=module=github.com/udisondev/molva \
	proto/*.proto

# TS-генерация для renderer'а (ts-proto); включается вместе с ui/.
if [ -d ui/node_modules/.bin ] && [ -x ui/node_modules/.bin/protoc-gen-ts_proto ]; then
	mkdir -p ui/src/gen
	protoc \
		--proto_path=proto \
		--plugin=protoc-gen-ts_proto=ui/node_modules/.bin/protoc-gen-ts_proto \
		--ts_proto_out=ui/src/gen \
		--ts_proto_opt=esModuleInterop=true,forceLong=bigint,oneof=unions \
		proto/*.proto
fi
