import pandas as pd

# Load data
file_path = "pagerank_benchmark.log"  # Change this if needed
df = pd.read_csv(file_path)

# Calculate the average execution time for each (Dataset, Mappers, Reducers) combination
avg_execution_time = df.groupby(["Dataset", "Mappers", "Reducers"])['Execution Time'].mean().reset_index()

# Define custom sorting order for datasets
custom_order = ["data-pr/input-mini.txt", "data-pr/input-small.txt", "data-pr/input-large.txt"]
avg_execution_time["Dataset"] = pd.Categorical(avg_execution_time["Dataset"], categories=custom_order, ordered=True)

# Sort by custom order
avg_execution_time = avg_execution_time.sort_values(by=["Dataset"])

# Save to file
avg_execution_time.to_csv("averaged_execution_time.txt", index=False, sep='\t')

print("Averaged execution times per (Dataset, Mappers, Reducers) combination have been computed, sorted (mini -> small -> large), and saved.")
