    
import pandas as pd
import io
import os
import dropbox
class ParquetMixin:
    def read_parquet(self, dbx_path: str, directory: str, filename: str, engine='pyarrow', **kwargs):
        """
        Downloads a Parquet file from Dropbox and loads it into a pandas DataFrame.

        Parameters
        ----------
        dbx_path : str
            The base Dropbox path where the file is stored.
        directory : str
            The directory within the base path where the file is stored.
        filename : str
            The name of the file (e.g., 'my_dataframe.parquet').
        engine : str, optional
            Engine to use for loading Parquet file. Default is 'pyarrow'.
        **kwargs
            Additional keyword arguments passed to `pandas.read_parquet`.

        Returns
        -------
        pandas.DataFrame or None
            The DataFrame loaded from the Parquet file, or None if an error occurs.
        """

        def loader(content: bytes, **loader_kwargs):
            buffer = io.BytesIO(content)
            return pd.read_parquet(buffer, **loader_kwargs)

        return self._base_read(
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            downloader=self.dbx.files_download,
            loader=loader,
            engine=engine,
            **kwargs
        )

    def write_parquet(self, df: pd.DataFrame, dbx_path: str, directory: str, filename: str, print_success=True, print_size=True, engine='pyarrow', **kwargs):
        """
        Saves a DataFrame to a Parquet file and uploads it to Dropbox.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame to save.
        dbx_path : str
            The base Dropbox path where the file will be saved.
        directory : str
            The directory within the base path where the file will be saved.
        filename : str
            The name of the file (e.g., 'my_dataframe.parquet').
        print_success : bool, optional
            Whether to print a success message upon successful upload.
        print_size : bool, optional
            Whether to print the file size before upload.
        **kwargs
            Additional keyword arguments to pass to `to_parquet`.
        """

        # Serialize DataFrame to Parquet bytes in memory
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine=engine, **kwargs)
        buffer.seek(0)
        parquet_content = buffer.getvalue()

        size_in_mb = len(parquet_content) / (1024 ** 2)
        if print_size:
            print(f"Size of the Parquet file: {size_in_mb:.2f} MB")

        # Define uploader callable to inject into _base_write
        def uploader(content: bytes, full_path: str):
            self.dbx.files_upload(
                content,
                full_path,
                mode=dropbox.files.WriteMode.overwrite
            )

        # Use CoreMixin's _base_write
        self._base_write(
            content=parquet_content,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            print_success=print_success
        )
