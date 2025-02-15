import os
import numpy as np
import pandas as pd


def load_and_display_metrics(region="sa-east-1", base_path="metrics_data"):
    region_path = os.path.join(base_path, region)

    if not os.path.exists(region_path):
        print(f"No data found for region {region}")
        return

    for file in os.listdir(region_path):
        if file.endswith(".npy"):
            table_name = file[:-4]  # Remove .npy extension
            file_path = os.path.join(region_path, file)

            # Load the numpy array
            data = np.load(file_path)

            # Convert to pandas DataFrame
            df = pd.DataFrame(data)

            # Rename columns for better readability
            df.columns = [col.replace("_", " ").capitalize() for col in df.columns]

            print(f"\nMetrics for table: {table_name}")
            print(df.to_string(index=False))
            print("\nSummary Statistics:")
            print(df.describe())
            print("\n" + "=" * 50 + "\n")


if __name__ == "__main__":
    load_and_display_metrics()
