import pandas as pd
import numpy as np

# Parameters
num_rows = 1000
num_cols = 4

# Generate random integers (e.g., between 0 and 1000)
data = np.random.randint(0, 1000, size=(num_rows, num_cols))
# Set the first column to 1
# data[:, 0] = 1
# Create column names like col_0, col_1, ..., col_249
columns = [f'col_{i}' for i in range(num_cols)]

# Create DataFrame and save to CSV
df = pd.DataFrame(data, columns=columns)
df.to_csv('data/integer_dataset.csv', index=False)
