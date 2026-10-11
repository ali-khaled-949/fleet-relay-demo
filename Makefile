.PHONY: test test-ui demo run worker check

test:
	python -m unittest discover -v

test-ui:
	node scripts/ui_smoke.js

demo:
	python -m scripts.demo --output artifacts/walkthrough.json

run:
	python -m uvicorn fleet_relay.app:app --host 127.0.0.1 --port 8000

worker:
	python -m fleet_relay.worker

check: test
	python -m compileall -q fleet_relay tests scripts
	node --check fleet_relay/static/app.js
