import pandas as pd
import numpy as np
import io

def generate_random_dataframe(size_mb: float, num_columns: int = 10, seed: int = None) -> pd.DataFrame:
        """
        Generate a random pandas DataFrame approximately of the specified size in megabytes.

        Parameters
        ----------
        size_mb : float
            Target size of the DataFrame in memory (in megabytes).
        num_columns : int, optional
            Number of columns in the DataFrame (default is 10).
        seed : int, optional
            Random seed for reproducibility (default is None).

        Returns
        -------
        df : pandas.DataFrame
            A DataFrame with random float data approximately matching the requested size.
        """
        if seed is not None:
            np.random.seed(seed)

        # Estimate how many rows are needed based on initial sample
        sample_rows = 1000
        sample_data = np.random.rand(sample_rows, num_columns)
        sample_df = pd.DataFrame(sample_data, columns=[f"col_{i}" for i in range(num_columns)])

        buffer = io.BytesIO()
        sample_df.to_csv(buffer)
        sample_size_mb = buffer.getbuffer().nbytes / (1024**2)

        approx_rows_needed = int((size_mb / sample_size_mb) * sample_rows)

        # Generate the full dataset
        full_data = np.random.rand(approx_rows_needed, num_columns)
        df = pd.DataFrame(full_data, columns=[f"col_{i}" for i in range(num_columns)])

        return df