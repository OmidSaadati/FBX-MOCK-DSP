#!/usr/bin/env python

import cgi
import json
import sys
import threading
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime
from urlparse import urlparse, parse_qs, parse_qsl

class RtbDspHandler(BaseHTTPRequestHandler):
    bid_values = {}
    win_notifs = {}
    error_notifs = {}
    index_html = ''
    req_res_list = []
    match_ids = []
    lock = threading.Lock()

    def process_win_notification(self, win_notif):
        sys.stderr.write('[RtbDsphandler] got win notification: %s\n'\
            % win_notif)
        try:
            request_id = win_notif['requestId']
            RtbDspHandler.win_notifs[request_id] = json.dumps(win_notif, indent=2)
        except Exception:
            return
    
    def process_error_notification(self, error_notif):
       sys.stderr.write('[RtbDspHandler] got error notfitication: %s\n'\
           % error_notif)
       try:
           request_id = error_notif['requestId']
           RtbDspHandler.error_notifs[request_id] = json.dumps(error_notif, indent=2)
       except Exception:
           return
       

    def write_bid_response(self, bid_request):
        sys.stderr.write('[RtbDspHandler] got bid request: %s\n'\
           % bid_request)
        try:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            try:
                json_bid = bid_request 
                request_id = json_bid['requestId']
                match_id = json_bid['partnerMatchId']
                RtbDspHandler.lock.acquire()
                json_response = RtbDspHandler.bid_values.get(match_id, {"bids":[]})
                RtbDspHandler.lock.release()
                json_response['requestId'] = request_id
                bid_response = json.dumps(json_response)
                sys.stderr.write('[RtbDspHandler] sent bid response: %s\n'\
                    % bid_response)
                self.wfile.write(bid_response)
                RtbDspHandler.req_res_list.insert(0, 
                    {'request_id': request_id, 'time': datetime.today().ctime(),\
                    'request':json.dumps(bid_request, indent=2), \
                    'response': json.dumps(json_response, indent=2)})
                if len(RtbDspHandler.req_res_list) > 25:
                    entry = RtbDspHandler.req_res_list.pop()
                    old_req_id = entry['request_id']
                    if old_req_id in RtbDspHandler.error_notifs:
                        del RtbDspHandler.error_notifs[old_req_id]
                    if old_req_id in RtbDspHandler.win_notifs:
                        del RtbDspHandler.win_notifs[old_req_id]
            except Exception:
                json_bid = {}
                RtbDspHandler.lock.release()
        except IOError:
            self.send_error(404, 'oops')

    def do_GET(self):
        sys.stderr.write('[RtbDspHandler] got get request: %s\n' % self.path)
        self.dispatch_request()

    def get_post_data(self):
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            postvars = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            postvars = cgi.parse_qs(self.rfile.read(length),
                                    keep_blank_values=1)
        elif ctype == 'application/json':
            length = int(self.headers.getheader('content-length'))
            postvars = json.loads(self.rfile.read(length))
        else:
            postvars = {}

        return postvars

    def do_POST(self):
        request_payload = self.get_post_data()
        sys.stderr.write('[RtbDspHandler] got post data: %s\n' \
                          % request_payload)
        if self.path == '/': # processing UI from values
           matchId = request_payload['matchId'][0]
           RtbDspHandler.match_ids.append(matchId)
           if len(RtbDspHandler.match_ids) > 100:
               RtbDspHandler.match_ids.pop()
           RtbDspHandler.lock.acquire()
           try:
               RtbDspHandler.bid_values[matchId] = json.loads('{"bids":[%s]}' % request_payload['bids'][0])
           except Exception:
               RtbDspHandler.bid_values[matchId] = {"bids":[]}
           RtbDspHandler.lock.release()
           self.send_response(302)
           self.send_header('Location', '/home?match_id=' + matchId)
           self.end_headers()
           self.wfile.write('')
        elif self.path == '/bids': # processing bid requests 
            bid_request = request_payload
            self.write_bid_response(bid_request)
        elif self.path == '/errors': # processing error notifications
            error_notification = request_payload
            self.process_error_notification(error_notification)
        elif self.path == '/wins': # / process win notifications
            win_notification = request_payload
            self.process_win_notification(win_notification)

    def render_form(self):
       self.send_response(200)
       self.send_header('Content-type', 'text/html')
       self.end_headers()
       if RtbDspHandler.index_html == '':
         f = open('index.html') 
         RtbDspHandler.index_html = f.read()
         f.close()
       matchId = ''
       query = parse_qs(urlparse(self.path).query)
       if 'match_id' in query:
           matchId = query['match_id'][0]
       RtbDspHandler.lock.acquire()
       json_response = RtbDspHandler.bid_values.get(matchId, {"bids":[]})
       RtbDspHandler.lock.release()
       json_response['requestId'] = 1234
       bid_response = json.dumps(json_response, indent=2)
       sys.stderr.write('match_id is %s \n' % matchId)
       bid_info = self.generate_bid_info()
       self.wfile.write(RtbDspHandler.index_html % \
           (cgi.escape(matchId), cgi.escape(bid_response), \
           cgi.escape(matchId, True), bid_info))
       

    def dispatch_request(self):
        if self.path.startswith('/home'):
            self.render_form()
        else:
           self.send_response(302)
           self.send_header('Location', '/home')
           self.end_headers()
           self.wfile.write('')
           

    def generate_bid_info(self):
      bid_info_table = ""
      for s in RtbDspHandler.req_res_list:
        error_notif = ''
        win_notif = ''
        if s['request_id'] in RtbDspHandler.error_notifs:
           error_notif = RtbDspHandler.error_notifs[s['request_id']]
        if s['request_id'] in RtbDspHandler.win_notifs:
           win_notif = RtbDspHandler.win_notifs[s['request_id']]
        bid_info_table += '<tr><td>'
        bid_info_table += \
            cgi.escape(s['time']) + '</td><td><pre>' + cgi.escape(s['request']) + \
            '</pre></td><td><pre>' + cgi.escape(s['response']) + '</pre></td><td><pre>' + \
            cgi.escape(error_notif) + '</pre></td><td><pre>' + cgi.escape(win_notif)
        bid_info_table += '</pre></td></tr>' 
      return bid_info_table
     

def cleanup():
    sys.stderr.write('in cleanup timer ...\n')
    RtbDspHandler.lock.acquire()
    for matchId in RtbDspHandler.bid_values.keys():
        if matchId not in RtbDspHandler.match_ids:
            del RtbDspHandler.bid_values[matchId]
    RtbDspHandler.lock.release()
    threading.Timer(30, cleanup).start()

def main():

    if sys.argv[1:]:
        port = int(sys.argv[1])
    else:
        port = 7777
    server_address = ('0.0.0.0', port)

    try:
        server = HTTPServer(server_address, RtbDspHandler)
        print 'started rtb dsp server on port %s ...' % port
        threading.Timer(30, cleanup).start()
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received, shutting down server'
        server.socket.close()

if __name__ == '__main__':
    main()

