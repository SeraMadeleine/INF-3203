#import "template/uit-assignment.typ": conf
#show: doc => conf(
  faculty: "Faculty of Science and Technology",
  title: "Assignment 2: ",
  subtitle: "Distributed log server",
  name: "Sera Madeleine Elstad",
  email: "sel063@uit.no",
  course: "INF-3203 Advanced Distributed Systems",
  semester: "Spring",
  year: "2025",
  doc,
)

= Introduction 
The goal of this assignment is to implement a consistent distributed log server that ensures reliable storage and ordering of log messages across multiple servers. Clients send log entries to any available server, and the system must coordinate between servers to maintain consistency, even in the presence of failures. The implementation should be fault-tolerant and capable of recovering from crashes while preserving a globally consistent log. To achieve this a distributed consensus algorithms, Raft, is implemented.

= Technical Background 
Raft is a fault-tolerant consensus algorithm designed for distributed systems. It simplifies the process by breaking it into leader election, log replication, and safety. A leader manages log updates, ensuring all servers maintain a consistent order. If the leader fails, a new one is elected through majority voting. Raft’s clarity and reliability make it ideal for ensuring consistency in our distributed log server #cite(<Raft>).

= Design and Implementation 
 

= Discussion 
 

= References
#bibliography("references.bib")
