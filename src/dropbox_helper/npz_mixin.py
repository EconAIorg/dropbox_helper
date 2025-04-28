import io
from scipy.sparse import load_npz, save_npz
import scipy
import dropbox

class NPZMixin:
    """
    Mixin to handle reading and writing sparse matrices (.npz files) to Dropbox.
    
    Provides methods for serializing sparse matrices and transferring them
    to and from Dropbox using the CoreMixin's base read/write infrastructure.
    """

    def write_npz(self, matrix: scipy.sparse.csr_matrix, dbx_path: str, directory: str, filename: str, print_success: bool = True, **kwargs):
        """
        Serialize and upload a sparse matrix as a `.npz` file to Dropbox.

        Parameters
        ----------
        matrix : scipy.sparse.csr_matrix
            The sparse matrix to save.
        dbx_path : str
            Base path in Dropbox where the file will be uploaded.
        directory : str
            Subdirectory within the base path where the file will be stored.
        filename : str
            The name of the `.npz` file to create (e.g., "matrix.npz").
        print_success : bool, optional
            If True, print a success message upon completion. Default is True.
        **kwargs
            Additional keyword arguments passed to `scipy.sparse.save_npz`.

        Returns
        -------
        None
        """
        buffer = io.BytesIO()
        save_npz(buffer, matrix, **kwargs)
        buffer.seek(0)

        # self.dbx.files_upload(buffer.getvalue(), full_dropbox_path, mode=dropbox.files.WriteMode.overwrite)

        self._base_write(
            content=buffer.getvalue(),
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=lambda content, full_path: self.dbx.files_upload(
                content,
                full_path,
                mode=dropbox.files.WriteMode.overwrite
            ),
            print_success=print_success
        )

    def read_npz(self, dbx_path: str, directory: str, filename: str):
        """
        Download and deserialize a `.npz` file from Dropbox into a sparse matrix.

        Parameters
        ----------
        dbx_path : str
            Base path in Dropbox where the file is located.
        directory : str
            Subdirectory within the base path where the file is stored.
        filename : str
            The name of the `.npz` file to download (e.g., "matrix.npz").

        Returns
        -------
        scipy.sparse.spmatrix or None
            The loaded sparse matrix, or None if an error occurs.
        """
        return self._base_read(
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            downloader=self.dbx.files_download,
            loader=self._load_sparse_matrix_from_bytes
        )

    @staticmethod
    def _load_sparse_matrix_from_bytes(file_bytes: bytes):
        """
        Helper function to load a sparse matrix from bytes.

        Parameters
        ----------
        file_bytes : bytes
            Byte content representing a saved `.npz` sparse matrix file.

        Returns
        -------
        scipy.sparse.spmatrix
            The deserialized sparse matrix object.
        """
        buffer = io.BytesIO(file_bytes)
        buffer.seek(0)
        return load_npz(buffer)

