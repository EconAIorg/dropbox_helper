import pytest
from dropbox_helper import dbx_helper
import os
import numpy as np
import io
import pandas as pd
import logging
import dropbox
import uuid
from dropbox.exceptions import ApiError


@pytest.fixture(scope="class", autouse=True)
def dropbox_test_folder(request):
    """
    Creates a unique test folder in Dropbox and deletes it after tests finish.
    Injects `self.dir` and `self.dbx_helper`.
    """
    folder_name = f"test_folder_{uuid.uuid4().hex}"
    helper = dbx_helper
    output_path = helper.output_path
    full_path = os.path.join(output_path, folder_name)

    # Create the folder in Dropbox
    helper.create_folder(full_path)

    # Inject into test class instance
    request.cls.dir = folder_name
    request.cls.dbx_helper = helper
    request.cls.output_path = output_path

    yield

    try:
        helper.dbx.files_delete_v2(full_path)
        print(f"✅ Deleted test folder: {full_path}")
    except ApiError as err:
        print("🛑 Error deleting test folder:", err)
        # swallow “not found,” but bubble up anything else
        if not (err.error.is_path_lookup() and err.error.get_path_lookup().is_not_found()):
            raise



@pytest.mark.usefixtures("dropbox_test_folder")
class TestHelloWorld:
    filename = "hello_world.txt"
    content = "hello world"
    
    @pytest.mark.order(1)
    def test_hello_world_upload(self):
        """Test uploading a simple 'hello world' file."""

        filename = "hello_world.txt"
        content = "hello world"
        test_path = self.dbx_helper._construct_path(
            self.output_path, self.dir, filename
        )

        file_bytes = self.content.encode('utf-8')

        self.dbx_helper.upload_file_directly(
            file_bytes=file_bytes,
            dbx_path=self.output_path,
            directory=self.dir,
            filename=self.filename,
            print_success=True
        )

        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.output_path, self.dir)
        )

        assert filename in files, f"{filename} not found in Dropbox folder!"

    @pytest.mark.order(2)
    def test_hello_world_download(self):
        """Test downloading and checking 'hello world' file."""

        file_content = self.dbx_helper.download_file_directly(
            dbx_path=self.output_path,
            directory=self.dir,
            filename=self.filename,
        )

        assert file_content is not None, "Downloaded file content is None!"
        decoded_content = file_content.decode('utf-8')
        assert decoded_content == self.content, f"File content mismatch: expected '{self.content}', got '{decoded_content}'"
