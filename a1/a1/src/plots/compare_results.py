import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data
file_path = "averaged_execution_time.txt"
df = pd.read_csv(file_path, delimiter="\t")

# Generate heatmaps for each dataset
datasets = df["Dataset"].unique()

# Set up the figure
fig, axes = plt.subplots(1, len(datasets), figsize=(20, 6))
fig.suptitle("Execution Time Heatmap: Mappers vs. Reducers")
vmin, vmax = df["Execution Time"].min(), df["Execution Time"].max()


for ax, dataset in zip(axes, datasets):
    subset = df[df["Dataset"] == dataset].pivot(index="Mappers", columns="Reducers", values="Execution Time")
    sns.heatmap(subset, annot=False, cmap="viridis", linewidths=0.5, ax=ax, vmin=vmin, vmax=vmax)
    ax.set_title(dataset)
    ax.set_xlabel("Reducers")
    ax.set_ylabel("Mappers")


# Save the plot
plot_file_path = "execution_time_heatmap.png"
plt.savefig(plot_file_path)
