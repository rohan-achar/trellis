import json
from urlparse import urlparse,parse_qs
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os, socket

import db

from threading import Thread

class QueryHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            queries = parse_qs(parsed.query)
            #print queries
            if "lat" in queries and "lon" in queries and "action" in queries:
                if queries["action"] == ["query"] and "maxd" in queries and "vc" in queries:
                    print float(queries["lat"][0])
                    print float(queries["lon"][0])
                    print float(queries["maxd"][0])

                    result = db.GetQuery(float(queries["lat"][0]), float(queries["lon"][0]), float(queries["maxd"][0]), int(queries["vc"][0]))
                    if result[0]:
                        self.send_response(200)
                        self.send_header('Content-type','application/json')
                        self.end_headers()
                        jsonResp = json.dumps(result[1])              
                        self.wfile.write(jsonResp)

                    else:
                        self.send_error(404, "Dont look for videos in the middle of nowhere, upload them instead")
                elif queries["action"] == ["save"]:
                    if "link" in queries and "thumbnail" in queries:
                        result = db.AddNewLink(float(queries["lat"][0]), float(queries["lon"][0]), queries["link"][0], queries["thumbnail"][0])
                        result = result.next()
                        if (result[0]):
                            self.send_response(200)
                            self.send_header('Content-type','application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({ "success" : True}))
                        
                    else:
                        self.send_error(440, "What do I save dumass?")
                else:
                    self.send_error(404, "No Idea what you are talking about")
            elif "action" in queries and "link" in queries:
                if queries["action"] == ["up"]:
                    result = db.UpdateRating(queries["link"][0], +1)
                elif queries["action"] == ["down"]:
                    result = db.UpdateRating(queries["link"][0], -1)
                else:
                    self.send_error(404, "Rate the video properly!")
                    return
                if (result[0]):
                    self.send_response(200)
                    self.send_header('Content-type','application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({ "success" : True}))
            else:
                self.send_error(404, "Put some shit in your request dumass!")
        except IOError:
            self.send_error(404,'What Shady Shit was tried?')
        except ValueError:
            self.send_error(404,'What Shady Shit was tried?')

def main(port):
    try:
        Thread(target=db.UpdateDlinks).start()
        server = HTTPServer(('', port), QueryHandler)
        print 'Loading up Awesome server.'
        server.serve_forever()
    except KeyboardInterrupt:
        print 'Bye Bye'
        server.socket.close()

if __name__ == '__main__':
    port = 4444
    if ('PORT' in os.environ):
        port = int(os.environ['PORT'])
    main(port)