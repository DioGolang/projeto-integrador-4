.PHONY: up down logs migrate-up migrate-down proto-gen lint

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

migrate-up:
	migrate -path migrations/ -database "postgres://root:password@localhost:5432/logistics?sslmode=disable" up

migrate-down:
	migrate -path migrations/ -database "postgres://root:password@localhost:5432/logistics?sslmode=disable" down

proto-gen:
	buf generate

lint:
	cd api && golangci-lint run
	cd web && npm run lint
