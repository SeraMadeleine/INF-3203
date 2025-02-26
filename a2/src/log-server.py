import sys
from urllib.parse import urlparse
import http.server
import socketserver
import signal
import socket
from raft import RaftStateMachine


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
raftStateMachine = RaftStateMachine(nodes_list)

class LogRequestHandler(http.server.SimpleHTTPRequestHandler):

    def do_PUT(self):
        global crashed, local_log
        
        # If the server is crashed, it should ignore incoming PUT requests and return immediately.
        if crashed:
            print(f"\n{self.server.server_address} Received PUT request while crashed, ignoring\n")
            return
        
        content_length = int(self.headers['Content-Length'])
        data = self.rfile.read(content_length).decode('utf-8')
        print(f"{self.server.server_address} Received PUT request with data: {data}")
        
        # Current logging logic is simple: it just appends the data to a list.
        local_log.append(data) # TODO: denne skal byttes ut med logikk for å legge til i loggen til RaftStateMachine
        # follower/candidates - send til leder 
            # Hvis leder ikke svarer - start election. Ta vare på i en lokal liste hvor du tar vare på info frem til leder sier ok 
        # Leader - append til log

        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        global crashed, local_log
        url = urlparse(self.path).path

        # If POST is extended, this case should be kept intact and overrule other URLs.
        if crashed and url != "/crash" and url != "/recover" and url != "/exit":
            print(f"\n{self.server.server_address} Received POST request while crashed, ignoring\n")
            return

        # If the server receives a POST request to /crash, it should simulate a crash.
        if url == "/crash":
            print(f"{self.server.server_address} Simulating crash...")
            crashed = True
            self.send_response(200)
            self.end_headers()
        
        # If the server receives a POST request to /recover, it should simulate a recovery.
        elif url == "/recover":
            print(f"{self.server.server_address} Simulating recovery...")
            crashed = False
            self.send_response(200)
            self.end_headers()

        # If the server receives a POST request to /exit, it should write its log to a file and exit.
        elif url == "/exit":
            print(f"{self.server.server_address} Exiting...")
            self.send_response(200)
            self.end_headers()
            print(f"{self.server.server_address}: {local_log}")
            with open(f"output/{output_id}-server-{self.server.server_address[0]}{self.server.server_address[1]}.csv", 'w') as f:
                for entry in local_log:
                    f.write(f"{entry}\n")

        elif url == "/rpc/appendEntries":
            # kun leader 
            print(f"{self.server.server_address} Received POST request to /rpc/appendEntries")
            
        
        elif url == "/rpc/requestVote":
            # kun candidate 
            print(f"{self.server.server_address} Received POST request to /rpc/requestVote")
            

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
