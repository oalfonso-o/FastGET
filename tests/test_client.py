from http.server import HTTPServer, BaseHTTPRequestHandler
import pytest
import threading
from time import sleep

from patata import Patata, Request, Response


servers_urls = {
    "200": "",
    "500": "",
    "wrong_headers": "",
    "timeout": "",
}


def run_server_200():
    global servers_urls

    class WebRequestHandler200(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write('{"status": "ok"}'.encode("utf-8"))

    server = HTTPServer(("", 0), WebRequestHandler200)
    servers_urls["200"] = f"http://{server.server_name}:{server.server_port}"
    server.serve_forever()


def run_server_500():
    global servers_urls

    class WebRequestHandler500(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write('{"status": "ko"}'.encode("utf-8"))

    server = HTTPServer(("", 0), WebRequestHandler500)
    servers_urls["500"] = f"http://{server.server_name}:{server.server_port}"
    server.serve_forever()


def run_server_wrong_headers():
    global servers_urls

    class WebRequestHandlerWrongHeaders(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.wfile.write('{"status": "no_end_headers"}'.encode("utf-8"))

    server = HTTPServer(("", 0), WebRequestHandlerWrongHeaders)
    servers_urls["wrong_headers"] = f"http://{server.server_name}:{server.server_port}"
    server.serve_forever()


def run_server_timeout():
    global servers_urls

    class WebRequestHandlerTimeout(BaseHTTPRequestHandler):
        def do_GET(self):
            sleep(100_000)

    server = HTTPServer(("", 0), WebRequestHandlerTimeout)
    servers_urls["timeout"] = f"http://{server.server_name}:{server.server_port}"
    server.serve_forever()


@pytest.fixture(autouse=True, scope="session")
def http_servers():
    thread_200 = threading.Thread(target=run_server_200)
    thread_200.daemon = True
    thread_200.start()
    thread_500 = threading.Thread(target=run_server_500)
    thread_500.daemon = True
    thread_500.start()
    thread_wrong_headers = threading.Thread(target=run_server_wrong_headers)
    thread_wrong_headers.daemon = True
    thread_wrong_headers.start()
    thread_timeout = threading.Thread(target=run_server_timeout)
    thread_timeout.daemon = True
    thread_timeout.start()


def test_response_200():
    with Patata() as client:
        responses = list(client.http("get", [Request(id_=1, url=servers_urls["200"])]))
        assert responses == [Response(id_=1, status_code=200, data={"status": "ok"})]


def test_response_200__retry():  # TODO
    pass


def test_response_500():
    with Patata() as client:
        responses = list(client.http("get", [Request(id_=1, url=servers_urls["500"])]))
        assert responses == [Response(id_=1, status_code=500, data={"status": "ko"})]


def test_response_500__retry():  # TODO
    pass


def test_server_doesnt_close_headers():
    with Patata() as client:
        responses = list(
            client.http("get", [Request(id_=1, url=servers_urls["wrong_headers"])])
        )
        assert responses[0].status_code == 500


def test_server_doesnt_close_headers__retry():  # TODO
    pass


def test_server_timeout():
    with Patata() as client:
        responses = list(
            client.http(
                "get", [Request(id_=1, url=servers_urls["timeout"])], timeout=0.1
            )
        )
        assert responses[0].status_code == 500
        assert "TimeoutError" in responses[0].data["exception_traceback"]


def test_server_timeout__retry():  # TODO
    pass
