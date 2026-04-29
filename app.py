from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os
import tempfile

PORT = int(os.environ.get("PORT", 10000))
UPLOAD_SECRET = os.environ.get("UPLOAD_SECRET", "dev-secret-key")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "dashboard_data_v2.json")


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "":
            self.path = "/dashboard_v2.html"
        return super().do_GET()

    def do_POST(self):
        if self.path != "/upload_cache_v2":
            self.send_error(404, "Not found")
            return

        # Authenticate
        if self.headers.get("X-Upload-Secret") != UPLOAD_SECRET:
            self._json_response(401, {"error": "Unauthorized"})
            return

        # Read body
        try:
            length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            self._json_response(400, {"error": "Invalid Content-Length"})
            return
        if length <= 0:
            self._json_response(400, {"error": "Empty body"})
            return

        raw = self.rfile.read(length)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            self._json_response(400, {"error": f"Invalid JSON: {exc}"})
            return

        if not isinstance(data, dict) or "global" not in data:
            self._json_response(400, {"error": "Malformed payload: missing 'global'"})
            return

        # Atomic write: temp file in same dir, then os.replace
        try:
            fd, tmp_path = tempfile.mkstemp(prefix=".dashboard_data_v2_", suffix=".json", dir=BASE_DIR)
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, DATA_PATH)
        except Exception as exc:
            self._json_response(500, {"error": f"Write failed: {exc}"})
            return

        self._json_response(200, {
            "ok": True,
            "lastUpdated": data.get("lastUpdated", ""),
            "bytes": len(raw),
        })

    def _json_response(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Purple Carrot Dashboard running on port {PORT}")
    server.serve_forever()
