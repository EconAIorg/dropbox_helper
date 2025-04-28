import io
import pickle
import dropbox

class PickleMixin:
    """
    Mixin for reading and writing Python pickle files via Dropbox.

    This class depends on `CoreMixin` methods `_base_read` and `_base_write`
    for abstracted file handling.

    Methods
    -------
    read_pickle(dbx_path, directory, filename)
        Downloads and deserializes a pickle file from Dropbox into a Python object.

    write_pickle(obj, dbx_path, directory, filename, print_success=True, print_size=True)
        Serializes a Python object into pickle format and uploads it to Dropbox.
    """

    def read_pickle(self,
                    dbx_path: str,
                    directory: str,
                    filename: str) -> object | None:
        """
        Download and deserialize a pickle file from Dropbox.

        Parameters
        ----------
        dbx_path : str
            Base Dropbox path where the file is located.
        directory : str
            Subdirectory within the base path containing the file.
        filename : str
            Name of the pickle file (e.g., 'model.pkl').

        Returns
        -------
        object or None
            The deserialized Python object if successful, otherwise None.
        """
        # loader: how to turn raw bytes into a Python object
        def loader(content: bytes):
            return pickle.loads(content)

        # simple full‐download; metadata ignored
        downloader = self.dbx.files_download

        return self._base_read(
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            downloader=downloader,
            loader=loader
        )


    def write_pickle(self,
                     obj: object,
                     dbx_path: str,
                     directory: str,
                     filename: str,
                     print_success: bool = True,
                     print_size: bool = True):
        """
        Serialize a Python object and upload it to Dropbox as a pickle file.

        Parameters
        ----------
        obj : object
            The Python object to serialize.
        dbx_path : str
            Base Dropbox path where the file should be saved.
        directory : str
            Subdirectory within the base path to save the file.
        filename : str
            Name of the output pickle file (e.g., 'model.pkl').
        print_success : bool, default=True
            Whether to print a success message after upload.
        print_size : bool, default=True
            Whether to print the size of the serialized file before upload.

        Returns
        -------
        None
        """
        # turn object → bytes
        buf = io.BytesIO()
        pickle.dump(obj, buf)
        content = buf.getvalue()

        if print_size:
            size_mb = len(content) / 1024**2
            print(f"Size of the pickle file: {size_mb:.2f} MB")

        # uploader: direct upload call
        uploader = lambda b, p: self.dbx.files_upload(
            b, p, mode=dropbox.files.WriteMode.overwrite
        )

        self._base_write(
            content=content,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            print_success=print_success
        )