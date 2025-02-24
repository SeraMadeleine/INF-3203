import subprocess
import os
import json
import time
import random  # Import random module

current_folder = os.path.dirname(os.path.abspath(__file__))
# config_path = os.path.join(current_folder, "word-count-config.json")
config_path = os.path.join(current_folder, "page-rank-config.json")
output_log = os.path.join(current_folder, "pagerank_benchmark.log")

# Function to get available nodes from the cluster
def get_available_nodes():
    try:
        # Run the command to list available nodes
        result = subprocess.run(["/share/ifi/available-nodes.sh"], capture_output=True, text=True, check=True)
        
        # Split output into a list of nodes
        nodes = result.stdout.strip().split("\n")
        
        # Return the list of available nodes
        return nodes if nodes else []
    except subprocess.CalledProcessError as e:
        print("Error retrieving available nodes:", e)
        return []

# Fetch available nodes from cluster 
available_nodes = get_available_nodes()

# Datasets to test
datasets = [
    #"data-wc/word-count-sanity-check.txt"
    "data-pr/input-large.txt",
    "data-pr/input-small.txt",
    "data-pr/input-mini.txt"
]

# Different configurations of mappers and reducers
mapper_counts = [2, 4, 8, 16]
reducer_counts = [2, 4, 8, 16]
runs_per_config = 20

# Running the tests with different combinations
with open(output_log, "w") as log_file:
    log_file.write("Dataset,Mappers,Reducers,Run,Execution Time\n")

    for dataset in datasets:
        for mappers in mapper_counts:
            for reducers in reducer_counts:
                for run in range(1, runs_per_config + 1):
                    # Modify config file
                    with open(config_path, "r") as f:
                        config_data = json.load(f)

                    # Select a random subset of available nodes
                    num_nodes = min(max(mappers, reducers), len(available_nodes))
                    selected_nodes = random.sample(available_nodes, num_nodes)

                    # Update the input dataset, mappers/reducers, and node selection
                    config_data["input_path"] = dataset
                    config_data["mappers"] = mappers
                    config_data["reducers"] = reducers
                    config_data["nodes"] = selected_nodes  # Assign random nodes

                    # Save the modified config file
                    with open(config_path, "w") as f:
                        json.dump(config_data, f, indent=4)

                    print(f"\nRunning test {run}/{runs_per_config} with {mappers} mappers and {reducers} reducers on {dataset} using nodes {selected_nodes}...")

                    # Define the command
                    command = [
                        "python3",
                        "mapreduce.py",
                        "--config_path",
                        config_path,
                        "--execution_mode",
                         "distributed"
                    ]

                    # Start the timer
                    start_time = time.time()
                    result = subprocess.run(command, capture_output=True, text=True)
                    
                    # Stop the timer
                    end_time = time.time()
                    elapsed_time = end_time - start_time  # Compute elapsed time

                    print(f"Execution Time: {elapsed_time:.4f} seconds")
                    log_file.write(f"{dataset},{mappers},{reducers},{run},{elapsed_time:.4f}\n")

print("\ndone, wrote to log file")
