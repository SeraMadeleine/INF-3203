import subprocess
import os

current_folder = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_folder, "word-count-config.json")

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

output_file = os.path.join(current_folder, "mr_tmp/output-word-count-sanity-check.txt")


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