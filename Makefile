.PHONY: up down logs eval eval-llm eval-confluent policy-test rebuild clean status inject check

up:            ## start the whole stack
	docker compose up --build -d
	@echo "dashboard  http://localhost:5173"
	@echo "api        http://localhost:8080/api/state"

confluent:     ## start against Confluent Cloud (fill KAFKA_* in .env first)
	docker compose -f docker-compose.yml -f docker-compose.confluent.yml up --build -d

down:
	docker compose down

clean:         ## down + remove volumes (wipes incident memory)
	docker compose down -v

logs:
	docker compose logs -f guardian detector

status:
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"
	@curl -s localhost:8080/api/cluster | python3 -m json.tool 2>/dev/null || true

inject:        ## make inject S=pool_exhaustion
	curl -s -X POST localhost:8082/inject/$(S) | python3 -m json.tool

check:         ## container-path imports + planner branch coverage
	python ops/import_check.py
	python ops/planner_check.py

eval: check
	python evals/run_eval.py

eval-llm:
	python evals/run_eval.py --llm

eval-confluent:
	python evals/run_eval.py --provider confluent

policy-test:
	opa test policy/ -v

rebuild:
	docker compose up -d --build
