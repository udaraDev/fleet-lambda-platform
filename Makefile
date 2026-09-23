.PHONY: up down logs test demo verify
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f streaming-raw streaming-speed producer-stream airflow-scheduler
test:
	python -m unittest discover -s tests -v
demo:
	python -m scripts.demo_local
verify:
	python -m unittest discover -s tests -v
	python -m scripts.verify_running --wait-seconds 480
