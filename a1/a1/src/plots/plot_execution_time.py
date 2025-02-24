import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the dataset
file_path = "averaged_execution_time.txt"  # Update to the correct file path
df = pd.read_csv(file_path, sep="\t")

# Drop any completely empty columns
df = df.dropna(axis=1, how="all")

# Ensure the first column is labeled properly
df.columns = ["Mappers", "Reducers", "Execution Time"]

# Convert numerical columns to correct types
df["Mappers"] = df["Mappers"].astype(int)
df["Reducers"] = df["Reducers"].astype(int)
df["Execution Time"] = df["Execution Time"].astype(float)

# Define heatmap range
vmin, vmax = df["Execution Time"].min(), 0.18

# Pivot data for heatmap
heatmap_data = df.pivot(index="Reducers", columns="Mappers", values="Execution Time")

# Plot heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(heatmap_data, annot=True, cmap="coolwarm", fmt=".4f", vmin=vmin, vmax=vmax)

# Labels and title
plt.title("Execution Time Heatmap: Mappers vs. Reducers")
plt.xlabel("Number of Mappers")
plt.ylabel("Number of Reducers")

# Save the plot
plot_file_path = "execution_time_heatmap.png"
plt.savefig(plot_file_path)





