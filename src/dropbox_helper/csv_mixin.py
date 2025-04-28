import pandas as pd
import io
import requests
import dropbox

class CSVMixin:
    """
    Mixin providing CSV read/write capabilities with Dropbox integration.

    Offers methods to read CSVs from Dropbox into pandas DataFrames and
    to write DataFrames as CSV files back to Dropbox, supporting both
    full and partial downloads as well as chunked uploads for large files.
    """

    def read_csv(self, dbx_path: str, directory: str, filename: str,
                 mb_to_load: int = None, **kwargs):
        """
        Read a CSV file from Dropbox into a pandas DataFrame.

        Parameters
        ----------
        dbx_path : str
            Base Dropbox path where the file is stored.
        directory : str or None
            Subdirectory within the base path. If None, file is at base path.
        filename : str
            Name of the CSV file to read (e.g., 'data.csv').
        mb_to_load : int or None, optional
            Maximum number of megabytes to load for a partial download.
            If None, the entire file is downloaded (default: None).
        **kwargs
            Additional keyword arguments passed to :func:`pandas.read_csv`,
            such as `sep`, `usecols`, `skiprows`, etc.

        Returns
        -------
        pandas.DataFrame or None
            DataFrame containing the CSV data, or None if an error occurred.
        """
        # loader: convert raw bytes into DataFrame
        # loader: turn raw bytes into a DataFrame
        def loader(content: bytes, **kwargs):
            return pd.read_csv(io.BytesIO(content), **kwargs)

        # downloader: full download via Dropbox SDK
        def downloader(full_path: str):
            return self.dbx.files_download(full_path)

        return self._base_read(
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            downloader=downloader,
            loader=loader,
            **kwargs
        )

    def write_csv(self, df: pd.DataFrame, dbx_path: str, directory: str,
                  filename: str, print_success: bool = True,
                  print_size: bool = True, **kwargs):
        """
        Write a pandas DataFrame to a CSV file and upload it to Dropbox.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame to save.
        dbx_path : str
            Base Dropbox path where the file will be uploaded.
        directory : str
            Subdirectory within the base path for saving the file.
        filename : str
            Name of the CSV file (e.g., 'output.csv').
        print_success : bool, optional
            Whether to print a success message upon completion (default: True).
        print_size : bool, optional
            Whether to print the file size before uploading (default: True).
        **kwargs
            Additional keyword arguments passed to :meth:`pandas.DataFrame.to_csv`,
            such as `index`, `header`, `sep`, etc.

        Returns
        -------
        None
            The DataFrame is uploaded; success or failure is printed or logged.
        """
        # 1) Bake the CSV into bytes
        buf = io.StringIO()
        df.to_csv(buf, **kwargs)
        data = buf.getvalue().encode("utf-8")

        # 2) Use your existing _base_write for a direct upload
        def uploader(content: bytes, full_path: str):
            self.dbx.files_upload(
                content,
                full_path,
                mode=dropbox.files.WriteMode.overwrite,
                autorename=False,
            )

        self._base_write(
            content=data,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            print_success=True,
        )
