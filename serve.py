import http.server, socketserver, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
print(f"Serving from {os.getcwd()}")
print("Open http://localhost:3000/viewer.html")
handler = http.server.SimpleHTTPRequestHandler
with socketserver.TCPServer(("", 3000), handler) as httpd:
    httpd.serve_forever()
