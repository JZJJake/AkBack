#!/usr/bin/env python3
import http.server
import socketserver
import json
import time

PORT = 8080

class MockTDXHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/server-status':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        elif '/api/minute-trade-all' in self.path:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            # Mock tick data
            data = {
                "code": 0,
                "data": {
                    "List": [
                        {"Volume": 100, "Status": 0},
                        {"Volume": 50, "Status": 1},
                        {"Volume": 20, "Status": 2}
                    ]
                }
            }
            self.wfile.write(json.dumps(data).encode())
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), MockTDXHandler) as httpd:
        print(f"Mock TDX Server serving on port {PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
