"""Bridge newline-delimited test requests to the real API, without sockets."""

import json
import sys
import tempfile
from pathlib import Path

# This script is started by Node using its absolute path, from any directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from fleet_relay.app import create_app


def main():
    with tempfile.TemporaryDirectory(prefix="fleet-ui-dom-") as temporary:
        with TestClient(create_app(Path(temporary) / "test.sqlite3")) as client:
            for line in sys.stdin:
                request_id = None
                try:
                    request = json.loads(line)
                    request_id = request["id"]
                    response = client.request(
                        request.get("method", "GET"),
                        request["path"],
                        headers=request.get("headers", {}),
                        content=request.get("body"),
                    )
                    result = {
                        "id": request_id,
                        "status": response.status_code,
                        "headers": dict(response.headers),
                        "body": response.json(),
                    }
                except Exception as exc:
                    result = {"id": request_id, "error": str(exc)}
                print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
