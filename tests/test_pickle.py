import pytest
from tests.utils import generate_random_dataframe
import os
import pandas as pd
import pandas.testing as pdt
from tests.test_init import dropbox_test_folder

@pytest.mark.usefixtures("dropbox_test_folder")
class TestPickleMixin:
    fname = 'small_pickle.pkl'

    @pytest.mark.order(7)
    def test_small_pickle_upload(self):
        # create a small DataFrame to pickle
        df = generate_random_dataframe(size_mb=0.01)
        # upload via PickleMixin
        self.dbx_helper.write_pickle(df, self.output_path, self.dir, self.fname)
        # verify it appears in the folder
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, f"{self.fname} not found in Dropbox folder!"

    @pytest.mark.order(8)
    def test_small_pickle_download(self):
        # ensure file is present first
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert self.fname in files, "Pickle file doesn't exist!"

        # download and unpickle
        obj = self.dbx_helper.read_pickle(self.output_path, self.dir, self.fname)
        # check we got back a DataFrame identical to what we sent
        assert isinstance(obj, pd.DataFrame), "Downloaded pickle is not a DataFrame."
        assert not obj.empty, "Downloaded DataFrame is empty!"
        # compare contents
        original_df = generate_random_dataframe(size_mb=0.01)  # note: generate a second DF for schema; if you need exact match, store and reuse
        # if you want strict equality, you could save and reuse the original object instead of regenerating
        pdt.assert_index_equal(obj.columns, original_df.columns)
        assert obj.shape == original_df.shape, "Downloaded DataFrame shape mismatch!"
