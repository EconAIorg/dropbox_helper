import pytest
from tests.utils import generate_random_dataframe
import os
import pandas as pd
import io
from tests.test_init import dropbox_test_folder

@pytest.mark.usefixtures("dropbox_test_folder")
class TestParquetMixin:
    fname = 'small_parquet.parquet'

    @pytest.mark.order(15)
    def test_small_parquet_upload(self):
        df = generate_random_dataframe(size_mb = .01)
        self.dbx_helper.write_parquet(df, self.output_path, self.dir, self.fname)
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, f"{self.fname} not found in Dropbox folder!"

    @pytest.mark.order(16)
    def test_small_parquet_download(self):
        # Test file exists
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, "File doesn't exist!"

        df = self.dbx_helper.read_parquet( self.output_path, self.dir, self.fname)
        assert isinstance(df, pd.DataFrame), "Downloaded parquet is not a DataFrame."
        assert not df.empty, "Downloaded DataFrame is empty!"