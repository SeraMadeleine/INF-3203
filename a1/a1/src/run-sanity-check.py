import subprocess
import os
import json

current_folder = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_folder, "page-rank-config.json")

command = [
    "python3", 
    "mapreduce.py", 
    "--config_path", 
    config_path,
    "--execution_mode", 
    "driver"
]

print("Running command:", " ".join(command))

result = subprocess.run(command, capture_output=True, text=True)

output_file = os.path.join(current_folder, "mr_tmp/output-pr.txt")


if result.stderr != "":
    print(result.stderr)
    print("Word count check: failure")
    exit(1)

search_string = "[\"LORD\", 30976]"

with open(output_file, 'r') as file:
    if search_string in file.read():
        print("Word count check: success")
    else:
        print("Word count check: error: Inaccurate count")


# ! Denne virker for B, men er en omskriving av den orginale koden 
# with open(output_file, 'r') as file:
#     search_key = ["B", 2.0] 

#     content = file.read()
#     print("Content of output file:\n", content)  # ! Only for debugging, can be removed 
    
#     # Parse as JSON
#     parsed_content = json.loads(content)  

#     # Only look at the first two values 
#     found = any(item[:2] == search_key for item in parsed_content)

#     if found:
#         print(f"Word count check: success - Found {search_key}")
#     else:
#         print(f"Word count check: error: Inaccurate count. Expected {search_key}, but not found.")
