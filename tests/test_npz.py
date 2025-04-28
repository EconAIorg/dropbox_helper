import pytest
import numpy as np
import os
import io
from scipy.sparse import random as sparse_random
from scipy.sparse import issparse
from tests.test_init import dropbox_test_folder

@pytest.mark.usefixtures("dropbox_test_folder")
class TestNPZMixin:
    fname = 'small_matrix.npz'

    @pytest.mark.order(11)
    def test_small_npz_upload(self):
        # Generate a small sparse matrix (10x10 with 20% density)
        matrix = sparse_random(10, 10, density=0.2, format='csr', dtype=np.float32)
        
        # Upload the matrix
        self.dbx_helper.write_npz(matrix, self.output_path, self.dir, self.fname)

        # Check that the file exists
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, f"{self.fname} not found in Dropbox folder!"

    @pytest.mark.order(12)
    def test_small_npz_download(self):
        # Check that the file already exists
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, "File doesn't exist for download test!"

        # Download and load the matrix
        matrix = self.dbx_helper.read_npz(self.output_path, self.dir, self.fname)

        # Validate it is a sparse matrix and non-empty
        assert issparse(matrix), "Downloaded object is not a sparse matrix!"
        assert matrix.shape[0] > 0 and matrix.shape[1] > 0, "Downloaded matrix is empty or has invalid shape!"
