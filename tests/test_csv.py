import pytest
from tests.utils import generate_random_dataframe
import os
import pandas as pd
import io
from tests.test_init import dropbox_test_folder


@pytest.mark.usefixtures("dropbox_test_folder")
class TestCSVMixin:
    small_name = 'small_csv.csv'
    large_name = 'large_csv.csv'

    @pytest.mark.order(3)
    def test_small_csv_upload(self):
        df = generate_random_dataframe(size_mb = .01)
        self.dbx_helper.write_csv(df, self.output_path, self.dir, self.small_name)
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.small_name in files, f"{self.small_name} not found in Dropbox folder!"

    @pytest.mark.order(4)
    def test_small_csv_download(self):
        # Check file exists:
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.small_name in files, "File doesn't exist!"

        df = self.dbx_helper.read_csv(self.output_path, self.dir, self.small_name)
        assert isinstance(df, pd.DataFrame), "Downloaded csv is not a DataFrame."
        assert not df.empty, "Downloaded DataFrame is empty!"

    @pytest.mark.order(5)
    def test_large_csv_upload(self):
        df = generate_random_dataframe(size_mb = 150)
        self.dbx_helper.write_csv(df, self.output_path, self.dir, self.large_name)
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.large_name in files, f"{self.large_name} not found in Dropbox folder!"

    @pytest.mark.order(6)
    def test_large_csv_download(self):
        # Check file exists:
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.large_name in files, "File doesn't exist!"

        df = self.dbx_helper.read_csv(self.output_path, self.dir, self.large_name)
        assert isinstance(df, pd.DataFrame), "Downloaded csv is not a DataFrame."
        assert not df.empty, "Downloaded DataFrame is empty!"
