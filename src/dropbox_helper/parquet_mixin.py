    
import pandas as pd
import io
import os
import dropbox

class ParquetMixin:
    def read_parquet(self, dbx_path: str, directory: str, filename: str, engine='pyarrow', **kwargs):
        """
        Downloads a Parquet file from Dropbox and loads it into a pandas DataFrame.

        Args:
        - dbx_path (str): The base Dropbox path where the file is stored.
        - directory (str): The directory within the base path where the file is stored.
        - filename (str): The name of the file (e.g., 'my_dataframe.parquet').

        Returns:
        - pandas.DataFrame: The DataFrame loaded from the Parquet file.
        """
        full_dropbox_path = os.path.join(dbx_path, directory, filename)
        try:
            # Download the Parquet file from Dropbox
            _, res = self.dbx.files_download(full_dropbox_path)

            # Load the content into a DataFrame
            buffer = io.BytesIO(res.content)
            df = pd.read_parquet(buffer, engine=engine, **kwargs)

            print(f"File '{filename}' successfully downloaded and loaded into a DataFrame.")
            return df
        except Exception as e:
            print(f"Failed to load Parquet file '{filename}' from Dropbox. Error: {e}")
            return None
    
    def write_parquet(self, df: pd.DataFrame, dbx_path: str, directory: str, filename: str, print_success=True, print_size=True, **kwargs):
        """
        Saves a DataFrame to a Parquet file and uploads it to Dropbox.

        Args:
        - df (pandas.DataFrame): The DataFrame to save.
        - dbx_path (str): The base Dropbox path where the file will be saved.
        - directory (str): The directory within the base path where the file will be saved.
        - filename (str): The name of the file (e.g., 'my_dataframe.parquet').
        - print_success (bool): Whether to print a success message upon successful upload.
        - print_size (bool): Whether to print the file size before upload.
        """
        full_dropbox_path = f"{dbx_path}/{directory}/{filename}"
        try:
            # Save DataFrame to a Parquet file in memory
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine='pyarrow', **kwargs)
            buffer.seek(0)
            parquet_content = buffer.getvalue()

            # Calculate file size
            size_in_bytes = len(parquet_content)
            size_in_mb = size_in_bytes / (1024 ** 2)  # Convert bytes to megabytes
            if print_size:
                print(f"Size of the Parquet file: {size_in_mb:.2f} MB")

            # Upload Parquet file to Dropbox
            if size_in_mb < 150:  # Dropbox API limit for direct upload
                self.dbx.files_upload(parquet_content, full_dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            else:
                self._chunked_upload_to_dropbox(parquet_content, full_dropbox_path)

            if print_success:
                print(f"File '{filename}' successfully uploaded to Dropbox path: '{full_dropbox_path}'")
        except Exception as e:
            print(f"Failed to upload Parquet file '{filename}' to Dropbox. Error: {e}")
