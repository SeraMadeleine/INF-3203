# MapReduce: Assignment 1 in INF-3203

## Project structure

```
├── doc/                        # Documentation
│   ├── assignment-text.pdf     # Assignment text
│   ├── paper.pdf               # MapReduce paper
│   ├── group.txt               # Group members
│   ├── report.txt              # Report for the assignment 
├── src/                        # Source files
│   ├── data-pr/                # Input data for Page Rank
│   │   ├── info.text
│   │   ├── input-large.txt
│   │   ├── input-letter.txt
│   │   ├── input-letter.txt
│   │   ├── input-small.txt
│   ├── data-wc/
│   │   ├── word-count-sanity-check.txt
│   ├── plots/                  # Scripts for result visualization
│   └── tests/                  # Tests
├── .gitignore                  # Files and folders ignored by Git
├── clean.sh                    # Shell script for cleaning temporary files
├── mapreduce.py                # Main MapReduce framework
├── page-rank-config.json       # Configuration for Page Rank job
├── page-rank-mapper.py         # Mapper for Page Rank
├── requirements.txt            # Python dependencies
├── run-sanity-check.py         # Sanity check script
├── word-count-config.json      # Configuration for Word Count job
└── word-count-mapper.py        # Mapper for Word Count
```

## 1. Connect to the Cluster

Use SSH to connect to the cluster:

```bash
ssh username@ificluster.ifi.uit.no
```

Replace `username` with your actual cluster username.

## 2. Identify a Free Compute Node

Run the following command to list available nodes:

```bash
/share/ifi/available-nodes.sh
```

Choose a free node from the list and connect to it using SSH:

```bash
ssh <node-name>
```

For example:

```bash
ssh c3-21
```

## 3. Locate the Project Directory

Navigate to the project folder:

```bash
cd /mnt/users/username/project-folder
```

Modify the path according to your project setup.

## 4. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 5. Run a Sanity Check

To verify the setup, run the following command:

```bash
python3 run-sanity-check.py
```

## 6. Running a MapReduce Job
### Wordcount 

### **Word Count**
1. Edit the input path for the `word-count-config.json` file to match your input data:
   ```json
   {
       "input_path": "src/data-wc/<input-file>.txt",
   }
   ```
2. Execute the Word Count job:
   ```bash
   python3 mapreduce.py word-count-config.json
   ```

### **Page Rank**
1. Edit the input path for the `page-rank-config.json` file for the input graph data:
   ```json
   {
       "input_path": "src/data-pr/<input-file>.txt",
   }
   ```
2. Execute the Page Rank job:
   ```bash
   python3 mapreduce.py page-rank-config.json
   ```

Modify the configuration file as needed (e.g., `word-count-config.json` or `page-rank-config.json`) and execute the MapReduce job using the appropriate script.

## 7. Running Tests
Tests are located in the `src/tests/` directory.
1. **Run Tests**
   ```bash
   python3 test.py
   ```

2. **Compute Average Execution Time**
   ```bash
   python3 average.py
   ```
   The script reads from `averaged_execution_time.txt` and computes averages for different datasets, mappers, and reducers.

3. **Generate Execution Time Plot**
   Visualize performance trends across datasets and configurations:
   ```bash
   python3 plot_execution_time.py
   ```
   This will produce a graphical representation of `averaged_execution_time.txt`.


## 8. Clean Up After Use

To free up cluster resources when done, terminate all running processes with:

```bash
/share/ifi/cleanup.sh
```

This ensures that no unnecessary jobs are left running on the cluster.

---
