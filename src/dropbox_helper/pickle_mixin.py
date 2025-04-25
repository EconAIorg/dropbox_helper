import io
import pickle
import dropbox

class PickleMixin:
    """
    Mixin for reading/writing Python pickles via BaseDropboxIO.
    """

    def read_pickle(self,
                    dbx_path: str,
                    directory: str,
                    filename: str) -> object | None:
        """
        Download and deserialize a pickle file from Dropbox.
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
        Serialize a Python object to pickle and upload it to Dropbox.
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

        # chunk if ≥150 MB
        chunked = (len(content) / 1024**2) >= 150

        self._base_write(
            content=content,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            chunked=chunked,
            print_success=print_success
        )