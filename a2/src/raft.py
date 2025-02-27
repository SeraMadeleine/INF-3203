import logging
import threading
import random
import requests
import time

# Configure logging to print debug messages
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

TIMEOUT_FOLLOWER_MIN = 5000 
TIMEOUT_FOLLOWER_MAX = 10000
TIMEOUT_CANDIDATE = 12000 
HEARTBEAT_INTERVAL = 2


class RaftStateMachine: 
    def __init__(self, nodes_list, address):
        self.state = "follower"
        self.term = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.nodes = nodes_list
        self.leader = None
        self.timer = None 
        self.address = address
        self.lastHeartbeat = None

        # starte election timer ved initialisering
        if self.address == self.nodes[0]:
            logging.info(f"Node {self.address} is the first in the list, starting election.")
            self.candidate()
        else:
            logging.info(f"Node {self.address} is waiting for leader heartbeats.")
            self.reset_follower_timeout()


    def __str__(self):
        return self.address
    
    # ---------- ENTRIES ---------- #
    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        logging.debug(f'Node {self}, \n term: {term}, \n leaderId: {leaderId}, \n prevLogIndex: {prevLogIndex}, \n prevLogTerm: {prevLogTerm}, \n entries: {entries}, \n leaderCommit: {leaderCommit}')
        if term >= self.term:
            for entry in entries:
                self.log.append(entry)
        
        if term > self.term:
            self.term = term
            self.leader = leaderId
            self.heartbeat()
            self.follower()
            return True
        
        if term == self.term and leaderId == self.leader:
            self.heartbeat()
            return True
        
        
            
        
        return False
        
    def receiveEntries(self, entries):
        if self.state == "leader":
            logging.debug(f"Leader {self} received new log entries: {entries}")
            self.log.append(entries)
            logging.info(f'log entries in recive entries: {self.log}')

            for node in self.nodes:
                if node != self.address:
                    try: 
                        requests.post(f"http://{node}/rpc/appendEntries", json={"term": self.term, "leaderId": self.address, "entries": entries}, timeout=2)
                    except requests.exceptions.RequestException:
                        logging.warning(f"Node {node} did not respond to appendEntries request from {self.address}")
        elif self.leader:
            logging.debug(f"Node {self} is forwarding log entries to leader {self.leader}")
            try:
                requests.put(f"http://{self.leader}/rpc/appendEntries", json={"entries": entries})
            except requests.exceptions.RequestException:
                logging.warning(f"Leader {self.leader} did not respond to appendEntries request from {self.address}")
                #TODO: Handle this better
                
        else:
            logging.warning(f"Node {self} does not know who the leader is. Dropping log entry.")


    # ---------- VOTES ---------- #

    def requestVote(self, node):
        try: 
            response = requests.post(f"http://{node}/rpc/requestVote", json={"term": self.term, "candidateId": self.address}, timeout=2)
            vote_granted = response.json().get("voteGranted", False)

            if vote_granted:
                logging.debug(f"Node {self.address} received vote from {node}")
                return True
            else:
                logging.debug(f"Node {node} rejected vote request from {self.address}")
                return False

        except requests.exceptions.RequestException:
            logging.warning(f"Node {node} did not respond to vote request from {self.address}")
            return False

    # ---------- STATE ---------- #


    def get_leader(self):
        # Update the leader information and state
        self.state = "leader"
        self.leader = self.address

        logging.info(f"Node {self} is now leader for term {self.term}")

        # Send heartbeats to all other nodes 
        for node in self.nodes:
            if node != self.address:
                try:
                    requests.post(f"http://{node}/rpc/leader", json={"leader": self.address}, timeout=2)
                except requests.exceptions.RequestException:
                    logging.warning(f"Could not inform {node} that {self.address} is leader")

        # Start sending heartbeats
        # threading.Thread(target=self.send_heartbeat, daemon=True).start()


    def follower(self): 
        self.state = "follower"
        logging.debug(f"Node {self} is now a follower")
        self.votedFor = None 

        # Start the follower timeout and reset the last heartbeat
        self.heartbeat()
        self.reset_follower_timeout()

    def candidate(self): 
        # If the node is already a leader, just return
        if self.state == "leader":
            return
        
        logging.debug(f"Node {self} is now a candidate")
        self.state = "candidate"
        self.term += 1
        self.votedFor = self.address
        votes = 1

        # Ask other nodes for their votes
        logging.info(f"Node {self} is requesting votes in term {self.term}")
        for node in self.nodes:
            if node != self.address: 
                success = self.requestVote(node)
                if success: 
                    votes += 1

            # ? må man passe på at den ikke går tilbake til follower fordi den får en heartbeat mens dette pågår?

        # If the node has received votes from a majority of the nodes, it becomes the leader, else it goes back to being a follower
        if votes > len(self.nodes) // 2:
            self.get_leader() 
            logging.info(f"Node {self} has become the leader for term {self.term}")
        else:
            logging.info(f"Node {self} did not receive enough votes, reverting to follower.")
            self.follower()

    

    # ---------- HEARTBEAT & TIMEOUTS ---------- #

    def heartbeat(self):
        self.lastHeartbeat = time.time()

    def send_heartbeat(self):
        if self.state != "leader":
            logging.warning(f"Node {self} is not a leader and cannot send heartbeats")
            return
        
        while self.state == "leader": 
            logging.debug(f"Leader {self} is sending heartbeats")

            for node in self.nodes:
                if node != self.address:
                    try: 
                        response = requests.post(f"http://{node}/rpc/appendEntries", json={"term": self.term, "leaderId": self.address, "entries": []}, timeout=2)
                        logging.debug(f"Leader {self} received response from {node}: {response.raw}")
                    except requests.exceptions.RequestException as e:
                        logging.warning(f"Node {node} did not respond to heartbeat request from {self.address}, error: {e}")
            

    def check_heartbeat(self):
        if self.state == "leader":
            self.send_heartbeat()
            return

        if self.lastHeartbeat is None or (time.time() - self.lastHeartbeat >  TIMEOUT_FOLLOWER_MAX):
            logging.debug(f"Node {self} has not received a heartbeat. Starting election.")
            self.candidate()

    # def check_heartbeat(self):
    #     if self.state == "leader":
    #         return

    #     if time.time() - self.lastHeartbeat > TIMEOUT_FOLLOWER_MAX:
    #         logging.debug(f"Node {self} has not received a heartbeat for {TIMEOUT_FOLLOWER_MAX} seconds")
    #         self.candidate()
    #     else: 
    #         self.reset_follower_timeout()

    def reset_follower_timeout(self):
        if self.timer is not None:
            self.timer.cancel()  
        
        timeout = random.uniform(TIMEOUT_FOLLOWER_MIN, TIMEOUT_FOLLOWER_MAX)
        logging.debug(f"Node {self.address} starts election timer for {timeout:.2f} seconds.")

        self.timer = threading.Timer(timeout, self.check_heartbeat)
        self.timer.start()

        

