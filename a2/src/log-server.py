import sys
from urllib.parse import urlparse
import http.server
import socketserver
import signal
import socket
from raft import RaftStateMachine
import logging
import json


# Configure logging to print debug messages
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')


''' 
Used to parse command line arguments, 
- output_id: used to name the output file
- address: host:port pair where the server will listen
- nodes_list: list of nodes that the client will send log entries to
'''
try:
    output_id, address, nodes_list = sys.argv[1], sys.argv[2], sys.argv[3:]

except IndexError:
    print("Usage: log-server.py <host:port>")
    sys.exit(1)

crashed = False     # Flag to simulate a crash
local_log = []      # List to store log entries
raftStateMachine = RaftStateMachine(nodes_list, address)

class LogRequestHandler(http.server.SimpleHTTPRequestHandler):

    def do_PUT(self):
        global crashed, local_log
        
        # If the server is crashed, it should ignore incoming PUT requests and return immediately.
        if crashed:
            logging.info(f"\n{self.server.server_address} Received PUT request while crashed, ignoring\n")
            return
        
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8').strip()
        # logging.debug(f"{self.server.server_address} Received PUT request with data: {data}")

        # If the data is a JSON object with an "entries" key, extract the value of the key.     
        if data.startswith('{"entries":'):
            try:
                data = json.loads(data)["entries"]  
            except json.JSONDecodeError:
                pass  

        # logging.debug(f"{self.server.server_address} Received PUT request aftert strippingwith data: {data}")

        raftStateMachine.receiveEntries(data)

        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        global crashed, local_log
        url = urlparse(self.path).path

        # If POST is extended, this case should be kept intact and overrule other URLs.
        if crashed and url != "/crash" and url != "/recover" and url != "/exit":
            logging.info(f"\n{self.server.server_address} Received POST request while crashed, ignoring\n")
            return

        # If the server receives a POST request to /crash, it should simulate a crash.
        if url == "/crash":
            logging.info(f"{self.server.server_address} Simulating crash...")
            crashed = True
            self.send_response(200)
            self.end_headers()
        
        # If the server receives a POST request to /recover, it should simulate a recovery.
        elif url == "/recover":
            logging.info(f"{self.server.server_address} Simulating recovery...")
            crashed = False
            self.send_response(200)
            self.end_headers()

        # If the server receives a POST request to /exit, it should write its log to a file and exit.
        elif url == "/exit":
            print(f"{self.server.server_address} Exiting...")

            if raftStateMachine.timer:
                raftStateMachine.timer.cancel()

            self.send_response(200)
            self.end_headers()
            
            print(f"{self.server.server_address}: {local_log}")
            with open(f"output/{output_id}-server-{self.server.server_address[0]}{self.server.server_address[1]}.csv", 'w') as f:
                for entry in raftStateMachine.log:
                    f.write(f"{entry}")

        
        elif url == "/rpc/appendEntries":
            # kun leader 
            logging.debug(f"{self.server.server_address} Received POST request to /rpc/appendEntries")
            content_length = int(self.headers['Content-Length'])

            # append entries kommer som json 
            if self.headers['Content-Type'] != 'application/json':
                logging.error(f"{self.server.server_address} Received POST request with invalid Content-Type")
                return
            
            data = self.rfile.read(content_length).decode('utf-8').strip()
            
            try: 
                data = json.loads(data)
                term = data.get("term")
                leaderId = data.get("leaderId")
                entries = data.get("entries", [])

                # Handle the case where entries is a string instead of a list
                if isinstance(entries, str):
                    entries = [entries]

                logging.debug(f"{self.server.server_address} Received POST request to /rpc/appendEntries with entries: {entries}")

            except json.JSONDecodeError:
                logging.error(f"{self.server.server_address} Received POST request with invalid JSON")
                self.send_response(400)
                self.end_headers()
                return
            
           

            success = raftStateMachine.appendEntries(term, leaderId, None, None, entries, None)
            response_data = json.dumps({"success": success})
            self.send_response(200)
            self.end_headers()

            logging.info(f"Node {self.server.server_address} received AppendEntries from {leaderId} and responded with {success}")



        elif url == "/rpc/leader":
            content_length = int(self.headers['Content-Length'])
            data = self.rfile.read(content_length).decode('utf-8').strip()

            try:
                request_data = json.loads(data)
                raftStateMachine.leader = request_data["leader"]
                logging.info(f"Node {self.server.server_address} recognizes {raftStateMachine.leader} as the leader")
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON in leader notification")
            
            self.send_response(200)
            self.end_headers()
    
  
        elif url == "/rpc/requestVote":
            # kun candidate 
            logging.debug(f"{self.server.server_address} Received POST request to /rpc/requestVote")      
            content_length = int(self.headers['Content-Length'])
            data = self.rfile.read(content_length).decode('utf-8').strip()

            try: 
                request_data = json.loads(data)
                term = request_data.get("term")
                candidate = request_data.get("candidateId")
            except json.JSONDecodeError:
                logging.error(f"{self.server.server_address} Received POST request with invalid JSON")
                self.send_response(400)
                self.end_headers()
                return

            vote_granted = False

            if term > raftStateMachine.term:
                raftStateMachine.term = term
                raftStateMachine.votedFor = candidate
                vote_granted = True
            else: 
                vote_granted = True 

            response_data = json.dumps({"voteGranted": vote_granted})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(response_data.encode())

            logging.debug(f"Node {self.server.server_address} voted {'YES' if vote_granted else 'NO'} for {candidate} in term {term}")



def start_server(address):
    ''' 
    Gets the host and port from the input argument,
    starts an HTTP server with the given address and port, and uses the LogRequestHandler class to handle requests.
    '''
    host, port = address.split(':')
    with socketserver.TCPServer((host, int(port)), LogRequestHandler) as server:
        print(f"Serving HTTP on {host} port {port}...")
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.serve_forever()

if __name__ == "__main__":
    start_server(address)
