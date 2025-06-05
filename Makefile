.PHONY: build init run migrate kill clear

SERVICE_NAME = app

build:
	docker-compose build || docker compose build

init:
	echo "Launching spaceship..."
	docker-compose up --build || docker compose up --build
	cat floppa.txt
	echo "Done! >:)"

run:
	docker-compose up || docker compose up

migrate:
	docker-compose run ${SERVICE_NAME} /app/sales-app || docker compose run ${SERVICE_NAME} /app/sales-app

kill:
	docker-compose down -v --remove-orphans --rmi local || docker compose down -v --remove-orphans --rmi local
	docker rmi $$(docker images -q ${SERVICE_NAME}) 2>/dev/null || true
	docker system prune -f
	cat floppa.txt
	echo "Done! >:)"

clear: kill
	docker volume prune -f
