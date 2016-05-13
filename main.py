from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from tools import classify
from urlparse import urlparse, parse_qsl
from sys import argv


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        #if not hasattr(current_thread(), "_children"):
        #    current_thread()._children = WeakKeyDictionary()
        
        que = dict(parse_qsl(urlparse(self.path).query))
        # for i in que:
        #     print i
        
        self.send_response(200)
        self.send_header("Content-type", "application/json")	
        
        res = classify(que['id1'], que['id2'])
   
        self.end_headers()
        self.wfile.write(res)

if __name__ == "__main__":
    server = HTTPServer(('',int(argv[1])),MyHandler)
    server.serve_forever()

