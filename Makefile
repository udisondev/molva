# Makefile molva — единая точка входа из корня репозитория.
# Ядро (Go) собирается и тестируется здесь; UI-цели проксируют в ui/ (npm),
# чтобы не заходить в папку вручную.

GO  ?= go
NPM ?= npm
UI  := ui
BIN := molvad

.DEFAULT_GOAL := help
.PHONY: help build vet test race check bench core proto ui-deps run dist clean

help: ## показать список целей
	@echo 'Цели (make <цель>):'
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

## --- Ядро (Go) ---

build: ## go build всех пакетов
	$(GO) build ./...

vet: ## go vet
	$(GO) vet ./...

test: ## go test всех пакетов
	$(GO) test ./...

race: ## тесты под детектором гонок
	$(GO) test -race ./...

check: build vet test ## сборка + vet + тесты (гейт перед коммитом)

bench: ## бенчмарки (медиапуть и транзакция входящего)
	$(GO) test ./... -run '^$$' -bench . -benchmem

core: ## собрать бинарь ядра molvad в корень
	$(GO) build -o $(BIN) ./cmd/molvad

proto: ## перегенерировать protobuf (Go + TS)
	sh proto/gen.sh

## --- UI (Electron) ---

ui-deps: ## установить зависимости UI (npm install)
	cd $(UI) && $(NPM) install

run: ## собрать ядро и запустить приложение
	cd $(UI) && $(NPM) start

dist: ## собрать пакеты (AppImage + pacman)
	cd $(UI) && $(NPM) run dist

## --- Обслуживание ---

clean: ## убрать артефакты сборки (бинарь, dist, release)
	rm -f $(BIN)
	rm -rf $(UI)/dist $(UI)/dist-electron $(UI)/build-daemon $(UI)/release
