.PHONY: up down logs test demo
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f streaming producer-stream airflow-scheduler
test:
	python -m unittest discover -s tests -v
demo:
	python -m scripts.demo_local
