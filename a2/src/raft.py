import logging
import threading
import random

# Configure logging to print debug messages
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')

TIMEOUT_FOLLOWER_MIN = 5 
TIMEOUT_FOLLOWER_MAX = 10 
TIMEOUT_CANDIDATE = 12 


class RaftStateMachine: 
    def __init__(self, nodes_list):
        self.state = "follower"
        self.term = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.nodes = nodes_list
        self.leader = None
        self.timer = None 

    def reset_follower_timeout(self):
        if self.timer is not None:
            self.timer.cancel()
        
        #? Litt usikker på dette, men vi vil i allefall ikke at timeouten skal være det samme for alle fordi det kan gi problemer med at alle blir kandidater samtidig
        timeout = random.randint(TIMEOUT_FOLLOWER_MIN, TIMEOUT_FOLLOWER_MAX)
        self.timer = threading.Timer(timeout, self.candidate)
        self.timer.start()
    
    def follower(self): 
        self.state = "follower"
        logging.debug(f"Node {self} is now a follower")
        self.votedFor = None 

        self.reset_follower_timeout()
        

    def candidate(self): 
        # If the node is already a leader, just return
        if self.state == "leader":
            return
        
        logging.debug(f"Node {self} is now a candidate")
        self.state = "candidate"
        self.term += 1
        self.votedFor = self
        votes = 1

        # Ask other nodes for their votes
        logging.debug(f"Node {self} is requesting votes")
        for node in self.nodes: 
            success = self.requestVote(node)
            if success: 
                votes += 1

            # ? må man passe på at den ikke går tilbake til follower fordi den får en heartbeat mens dette pågår?

        # If the node has received votes from a majority of the nodes, it becomes the leader
        if votes > len(self.nodes) // 2:
            self.leader()
            logging.info(f"Node {self} has become the leader for term {self.term}")
        else:
            self.follower()

    def requestVote(self, node):
        logging.debug(f"Requesting vote from {node}")
        try: 
            logging.debug(f"Node {self} received vote from {node}")
            return True 
        except: 
            logging.info(f"Node {node} did not vote for {self}")
            return False


    def leader(self):
        self.state = "leader"

        # TODO: starte en heartbeat timer som sender ut heartbeats til alle andre noder



    def timeout(self, timeoutLength):
        if self.state == "follower":
            self.candidate()

        elif self.state == "candidate":
            self.candidate()
            


