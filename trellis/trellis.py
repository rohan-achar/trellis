import json, time
from urlparse import urlparse,parse_qs
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os, socket

import db

from threading import Thread, Lock

filelock = Lock()

def log(device, type, path):
    with filelock:
        file = open("log.csv", "a")
        file.write(time.strftime("%c", time.gmtime()) + "," + device + "," + type + "," + path + "\n")
        file.close()

class QueryHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            queries = parse_qs(parsed.query)
            if "lat" in queries and "lon" in queries and "action" in queries:
                if queries["action"] == ["save"]:
                    length= int( self.headers['content-length'] )
                    data = json.loads(self.rfile.read(length))
                    self.rfile.close()
                    if "link" in data and "thumbnail" in data and "video_id" in data:
                        result = db.AddNewLink(float(queries["lat"][0]), float(queries["lon"][0]), data["link"], data["thumbnail"], data["video_id"])
                        if (result[0]):
                            self.send_response(200)
                            self.send_header('Content-type','application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({ "success" : True}))
                    else:
                        self.send_error(440, "What do I save dumass?")
                else:
                    self.send_error(404, "No Idea what you are talking about")
            else:
                self.send_error(404, "Really? No lat or lon or action?")
            if "device_id" in queries:
                log(queries["device_id"][0], "post", self.path)
        except IOError:
            self.send_error(404,'What Shady Shit was tried?')
        except ValueError:
            self.send_error(404,'What Shady Shit was tried?')

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
                    if "link" in queries and "thumbnail" in queries and "video_id" in queries:
                        result = db.AddNewLink(float(queries["lat"][0]), float(queries["lon"][0]), queries["link"][0], queries["thumbnail"][0], queries["video_id"][0])
                        if (result[0]):
                            self.send_response(200)
                            self.send_header('Content-type','application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({ "success" : True}))
                        
                    else:
                        self.send_error(440, "What do I save dumass?")
                else:
                    self.send_error(404, "No Idea what you are talking about")
            elif "action" in queries and "video_id" in queries:
                if queries["action"] == ["up"]:
                    result = db.UpdateRating(queries["video_id"][0], +1)
                elif queries["action"] == ["down"]:
                    result = db.UpdateRating(queries["video_id"][0], -1)
                elif queries["action"] == ["report"]:
                    result = db.UpdateReport(queries["video_id"][0])
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
            if "device_id" in queries:
                log(queries["device_id"][0], "GET", self.path)
        except IOError:
            self.send_error(404,'What Shady Shit was tried?')
        except ValueError:
            self.send_error(404,'What Shady Shit was tried?')

def main(port):
    try:
        t = Thread(target=db.UpdateDlinks)
        t.setDaemon(True)
        t.start()
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