# Assignment 2: Distributed log server

## How to run:
```sh
cd src
chmod +x run.sh
./run.sh
```
This assigns a new `output_id` to the client and all servers based on the current timestamp.

## Check log correctness:
```sh
python3 log-comparer.py <output_id>
```
The outputId will be printed in the terminal after the program has exited. 

## Clean the output folder:
```sh
./clean-output.sh
```
This script removes all log files from the output folder.

## Kill running servers and clients:

To stop all running server and client processes, run:

```sh
pkill -f "python3 log-server.py"
pkill -f "python3 log-client.py"
```

This command finds and terminates all running instances of `log-server.py` and `log-client.py`.

## NOTE:
I have modified how the client sends requests. Everything is now sent as JSON. This change was made to simplify and ensure consistent handling.

---

## File Structure:

```
A2/
├── doc/
│   ├── Report
│   └── Assignment_2_ADS.pdf
│
├── src/
│   ├── output/
│   │   ├── <timestamp>-client.csv
│   │   ├── <timestamp>-server-127.0.0.1.<port>.csv
│   │   ├── <timestamp>-server-127.0.0.1.<port>.csv
│   │   ├── <timestamp>-server-127.0.0.1.<port>.csv
│   │   └── ...
│   ├── clean-output.sh
│   ├── kill-local.sh
│   ├── log-client.py
│   ├── log-comparer.py
│   ├── log-server.py
│   ├── raft.py
│   ├── run.sh
│   ├── .gitignore
└── README.txt
```

