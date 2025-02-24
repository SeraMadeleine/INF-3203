import argparse
import importlib.util
from math import ceil
import sys
import os
import json
import uuid
import socket
from datetime import datetime
from fabric import Connection
from concurrent.futures import ThreadPoolExecutor, as_completed

class Config:
    def __init__(self, config_path, config_json):
        current_folder = os.path.dirname(os.path.abspath(__file__))

        self.config_path = config_path
        self.mr_def_path = os.path.join(current_folder, config_json.get('mr_def_path'))
        self.mappers = config_json.get('mappers')
        self.reducers = config_json.get('reducers')
        self.input_path = os.path.join(current_folder, config_json.get('input_path'))
        self.nodes = config_json.get('nodes')
        self.tmp_folder = os.path.join(current_folder, config_json.get('tmp_folder'))
        self.mr = None
        self.local = config_json.get('local')
        self.input_scale = config_json.get('input_scale')
        self.output_file = os.path.join(current_folder, config_json.get('output_file'))

        if not os.path.exists(self.tmp_folder):
            os.makedirs(self.tmp_folder)


def list_and_split_input(config):
    input_path = config.input_path
    n_chunks = config.mappers

    if os.path.isdir(input_path):
        print(f"Input files folder not found: {input_path}")
        sys.exit(1)

        files = [os.path.join(input_path, f) for f in os.listdir(input_path) if os.path.isfile(os.path.join(input_path, f))]
        files = files * config.input_scale

        return split_file_list(files, n_chunks)
    elif os.path.isfile(input_path):
        return split_file(input_path, config.mappers, config)
    else:
        print(f"Input path not found: {input_path}")
        sys.exit(1)

def split_file(file, n_chunks, config):
    file_handles = [open(os.path.join(config.tmp_folder, f"tmp_input_{os.path.basename(file)}_part_{i}"), 'w') for i in range(n_chunks)]

    for i in range(config.input_scale):
        with open(file, 'r') as f:
            for i, line in enumerate(f):
                file_handles[i % n_chunks].write(line)

    for fh in file_handles:
        fh.close()

    return [[os.path.abspath(fh.name)] for fh in file_handles]

def split_file_list(files, n_chunks):
    chunk_size = len(files) // n_chunks
    remainder = len(files) % n_chunks
    chunks = []
    start = 0

    for i in range(n_chunks):
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(files[start:end])
        start = end

    return chunks

def load_config(config_path):
    if not os.path.isfile(config_path):
        print(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path, 'r') as f:
        config = json.load(f)
        return Config(config_path, config)

def load_mr_module(config):
    module_path = config.mr_def_path

    if not module_path or not os.path.isfile(module_path):
        print(f"Module file not found: {module_path}")
        sys.exit(1)

    module_name = os.path.splitext(os.path.basename(module_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    assert hasattr(module, 'mapper'), f"The module {module_path} does not have a function named 'mapper'"
    assert hasattr(module, 'reducer'), f"The module {module_path} does not have a function named 'reducer'"

    config.mr = module


def get_random_file_name(prefix, temporary=True):
    if temporary:
        prefix = "tmp_" + prefix
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{timestamp}_{uuid.uuid4().hex[:8]}.json"

def do_mapping(files, config, output_file):
    with open(output_file, 'w') as of:
        of.write('[')
        first = True
        for file in files:
            with open(file, 'r') as f:
                for line in f:
                    output_list = config.mr.mapper(line.strip())
                    for output in output_list:
                        if not first:
                            of.write(',')
                        json.dump(output, of)
                        first = False
        of.write(']')

def do_reducing(files, config, output_file):
    intermediate_data = []
    for file in files:
        with open(file, 'r') as f:
            intermediate_data.extend(json.load(f))

    reduced_data = config.mr.reducer(intermediate_data)

    with open(output_file, 'w') as f:
        json.dump(reduced_data, f)

def remote_map(node, chunk, config, output_file):
    print("Mapping on node: ", node)
    conn = Connection(node)
    remote_files = ','.join(chunk)
    script = f'python3 {os.path.abspath(__file__)} --config_path {config.config_path} --execution_mode map --intermediate_files {remote_files} --tmp_output_file {output_file}'
    result = conn.run(script, hide=False)
    return result.stdout

def remote_reduce(node, chunk, config, output_file):
    print("Reducing on node: ", node)
    conn = Connection(node)
    remote_files = ','.join(chunk)
    script = f'python3 {os.path.abspath(__file__)} --config_path {config.config_path} --execution_mode reduce --intermediate_files {remote_files} --tmp_output_file {output_file}'
    result = conn.run(script, hide=False)
    return result.stdout

def start_remote_operation(f, list_of_file_sets, config, output_files):
    assert len(list_of_file_sets) == len(output_files)

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(f, config.nodes[i % len(config.nodes)], chunk, config, output_files[i]): chunk for i, chunk in enumerate(list_of_file_sets)}
        results = []
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


def do_sort_before_reduce(files, config):
    total_files_split = config.reducers

    intermediate_data = []
    for file in files:
        with open(file, 'r') as f:
            intermediate_data.extend(json.load(f))

    sorted_files = [os.path.join(config.tmp_folder, get_random_file_name("sorted")) for _ in range(total_files_split)]
    buffers = [[] for _ in range(total_files_split)]

    for key, value in intermediate_data:
        key_hash = hash(key) % total_files_split
        buffers[key_hash].append((key, value))

    for i, sorted_file in enumerate(sorted_files):
        with open(sorted_file, 'w') as f:
            json.dump(buffers[i], f)

    return sorted_files


def debug_merge_json_files(files, output_file):
    with open(output_file, 'w') as of:
        of.write('[')
        first = True
        for file in files:
            with open(file, 'r') as f:
                data = json.load(f)
                for item in data:
                    if not first:
                        of.write(',')
                    json.dump(item, of)
                    first = False
        of.write(']')

def debug_merge_files(files, output_file):
    with open(output_file, 'w') as f:
        for file in files:
            with open(file, 'r') as f2:
                for line in f2:
                    f.write(line)

def clean_temporary_files(config):
    for filename in os.listdir(config.tmp_folder):
        if filename.startswith("tmp"):
            file_path = os.path.join(config.tmp_folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

def driver(config):

    # List of list of files - each sublist is processed by an individual mapper
    files = list_and_split_input(config)
    print("\n\nFiles to process: ", files)


    # List of files - each file corresponds to one mapper output
    map_out = [os.path.join(config.tmp_folder, get_random_file_name("mapper")) for _ in range(config.mappers)]
    print(f"\n\nMapper output files: {map_out}")


    start_remote_operation(remote_map, files, config, map_out)

    sorted_files = do_sort_before_reduce(map_out, config)
    print(f"\n\nSorted files: {sorted_files}")


    reduce_out = [os.path.join(config.tmp_folder, get_random_file_name("reducer")) for _ in range(config.reducers)]
    print(f"\n\nReducer output files: {reduce_out}")

    chunked_sorted_files = split_file_list(sorted_files, config.reducers)
    print(f"\n\nchunked_sorted_files: {chunked_sorted_files}")
    

    start_remote_operation(remote_reduce, chunked_sorted_files, config, reduce_out)

    output_path = config.output_file if config.output_file != "" else get_random_file_name("final_output")
    debug_merge_json_files(reduce_out, os.path.join(config.tmp_folder, output_path))

    clean_temporary_files(config)

    print(f"output: {output_path}")


if __name__ == "__main__":
    hostname = socket.gethostname()
    if hostname == 'ificluster.ifi.uit.no':
        print("Error: Do not run this script on the cluster head node. Use the compute nodes instead.", file=sys.stderr)
        exit(1)

    parser = argparse.ArgumentParser(description="Run a MapReduce job.")
    parser.add_argument("--config_path", type=str, required=True, help="Path to the configuration file.")
    parser.add_argument("--execution_mode", type=str, choices=["driver", "map", "reduce"], help="Execution mode: 'driver', 'map', or 'reduce'. When starting your job, use the 'driver' option. 'map' and 'reduce' are typically used internally when scheduling these operations on remote nodes.")
    parser.add_argument("--intermediate_files", type=str, nargs='?', help="Files to process. Should typically not be specified manually. Use the input_files_folder field in the configuration file instead.")
    parser.add_argument("--tmp_output_file", type=str, nargs='?', help="Path to expected intermediate output file during mapping/redcing.")


    args = parser.parse_args()
    config = load_config(args.config_path)
    load_mr_module(config)

    print(f"Running {args.execution_mode} on {socket.gethostname()}")

    if args.execution_mode == "driver":
        driver(config)
    elif args.execution_mode == "map":
        do_mapping(args.intermediate_files.split(','), config, args.tmp_output_file)
    elif args.execution_mode == "reduce":
        do_reducing(args.intermediate_files.split(','), config, args.tmp_output_file)


