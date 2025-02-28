import logging
import threading
import random
import requests
import time

# Configure logging to print error, warning, info and/or debug messages
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')

# Timeout values for the different states in the Raft algorithm (milliseconds)
TIMEOUT_FOLLOWER_MIN = 5000  # 5 seconds
TIMEOUT_FOLLOWER_MAX = 10000 # 10 seconds
TIMEOUT_CANDIDATE = 12000    # 12 seconds
HEARTBEAT_INTERVAL = 2       # 2 seconds


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

        # Start election timer upon initialization
        if self.address == self.nodes[0]:
            logging.info(f"Node {self.address} is the first in the list, starting election.")
            self.state_candidate()
        else:
            logging.info(f"Node {self.address} is waiting for leader heartbeats.")
            self.reset_follower_timeout()


    def __str__(self):
        return self.address
    

    # ---------- ENTRIES ---------- #
    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        logging.debug(f'Node {self}, \n term: {term}, \n leaderId: {leaderId}, \n prevLogIndex: {prevLogIndex}, \n prevLogTerm: {prevLogTerm}, \n entries: {entries}, \n leaderCommit: {leaderCommit}')
        
        # If the received term is valid (greater than or equal to the current term), add the entries to the log
        if term >= self.term:
            for entry in entries:
                # prevent duplicates
                if entry not in self.log:
                    self.log.append(f'{entry}\n')

        # If the received term is greater than the current term, update the term and leader information             
        if term > self.term:
            self.term = term
            self.leader = leaderId
            self.heartbeat()
            self.state_follower()
            return True
        
        # If the term and the leader is the same as the current term and leader, update the heartbeat
        if term == self.term and leaderId == self.leader:
            self.heartbeat()
            return True
        
        return False
        

    def receiveEntries(self, entries):
        # Handles log entries received from the leader or forwards them if the node is not the leader
        if self.state == "leader":
            logging.debug(f"Leader {self} received new log entries: {entries}")
            self.log.append(f'{entries}\n')     

            logging.info(f'log entries in recive entries: {self.log}')

            # Send the entries to the followers
            for node in self.nodes:
                if node != self.address:
                    try: 
                        requests.post(f"http://{node}/rpc/appendEntries", json={"term": self.term, "leaderId": self.address, "entries": entries}, timeout=2)
                    except requests.exceptions.RequestException:
                        logging.warning(f"Node {node} did not respond to appendEntries request from {self.address}")
        # If the node is a follower, forward the entries to the leader 
        elif self.leader:
            logging.debug(f"Node {self} is forwarding log entries to leader {self.leader}")
            try:
                requests.put(f"http://{self.leader}/rpc/appendEntries", json={"entries": entries})
            except requests.exceptions.RequestException:
                logging.warning(f"Leader {self.leader} did not respond to appendEntries request from {self.address}")                
        # If the node does not know who the leader is, drop the entries
        else:
            logging.warning(f"Node {self} does not know who the leader is. Dropping log entry.")
            #TODO: handle this better, maybe by returning an error message to client



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

    def state_leader(self):
        # Transition the node to leader state and send heartbeats to maintain leadership
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


    def state_follower(self): 
        # Transition the node to follower state and reset election timers   
        self.state = "follower"
        logging.debug(f"Node {self} is now a follower")
        self.votedFor = None 

        # Start the follower timeout and reset the last heartbeat
        self.heartbeat()
        self.reset_follower_timeout()

    def state_candidate(self): 
        # Transition the node to candidate state and initiate a new election

        # If the node is already a leader, it should not become a candidate
        if self.state == "leader":
            logging.warning(f"Node {self} is already a leader and cannot become a candidate")
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
            self.state_leader() 
            logging.info(f"Node {self} has become the leader for term {self.term}")
        else:
            logging.info(f"Node {self} did not receive enough votes, reverting to follower.")
            self.state_follower()

    

    # ---------- HEARTBEAT & TIMEOUTS ---------- #

    def heartbeat(self):
        # Update when the last heartbeat was received
        self.lastHeartbeat = time.time()

    def send_heartbeat(self):
        # If the node is not a leader, it cannot send heartbeats and should return
        if self.state != "leader":
            logging.warning(f"Node {self} is not a leader and cannot send heartbeats")
            return
        
        # As long as the node is a leader, it should send heartbeats to all other nodes
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
        # If the node is a leader, it should send heartbeats
        if self.state == "leader":
            self.send_heartbeat()
            return

        # If the node is a follower, it should check if it has received a heartbeat. If not, it should start an election
        if self.lastHeartbeat is None or (time.time() - self.lastHeartbeat >  TIMEOUT_FOLLOWER_MAX):
            logging.debug(f"Node {self} has not received a heartbeat. Starting election.")
            self.state_candidate()


    def reset_follower_timeout(self):
        if self.timer is not None:
            self.timer.cancel()  
        
        timeout = random.uniform(TIMEOUT_FOLLOWER_MIN, TIMEOUT_FOLLOWER_MAX)
        logging.debug(f"Node {self.address} starts election timer for {timeout:.2f} seconds.")

        self.timer = threading.Timer(timeout, self.check_heartbeat)
        self.timer.start()

        

