import dropbox
import logging
import os

class CoreMixin:
    """
    Mixin providing core Dropbox file and folder management operations.

    Provides methods to initialize paths, check for folder existence,
    create folders, list files, and upload files or logs.
    """

    def __init__(self, dbx_token, dbx_key, dbx_secret, input_path = '/input', output_path = '/output', custom_paths=False):
        """
        Initialize the CoreMixin with Dropbox authentication and paths.

        Parameters
        ----------
        dbx_token : str
            OAuth2 refresh token for Dropbox API.
        dbx_key : str
            App key for Dropbox API.
        dbx_secret : str
            App secret for Dropbox API.
        input_path : str
            Base path in Dropbox for input data.
        output_path : str
            Base path in Dropbox for output data.
        custom_paths : bool, optional
            Whether to use custom input/output paths, by default False.
        """
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=dbx_token,
            app_key=dbx_key,
            app_secret=dbx_secret,
        )
        self.input_path = input_path
        self.output_path = output_path
        self.custom_paths = custom_paths
    
    def _construct_path(self, dbx_path: str, directory: str, filename: str) -> str:
        return os.path.join(dbx_path, directory, filename)

    def _base_read(self,
                   dbx_path: str,
                   directory: str,
                   filename: str,
                   downloader: callable,
                   loader: callable,
                   **loader_kwargs):
        """
        Generic downloader + loader wrapper.
        """
        full_path = self._construct_path(dbx_path, directory, filename)
        try:
            _, res = downloader(full_path)
            return loader(res.content, **loader_kwargs)
        except Exception as e:
            print(f"Error reading '{filename}' from Dropbox: {e}")
            return None

    def _base_write(self,
                    content: bytes,
                    dbx_path: str,
                    directory: str,
                    filename: str,
                    uploader: callable,
                    print_success: bool = True):
        full_path = self._construct_path(dbx_path, directory, filename)
        try:
            # Determine if we should chunk based on content size
            if len(content) >= 150 * 1024 * 1024: # chunk if ≥150 MB
                self._chunked_upload_to_dropbox(content, full_path)
            else:
                uploader(content, full_path)

            if print_success:
                print(f"Uploaded '{filename}' to '{full_path}'")
        except Exception as e:
            print(f"Error uploading '{filename}' to Dropbox: {e}")
    
    def _chunked_upload_to_dropbox(self, content, full_dropbox_path, chunk_size=25 * 1024 * 1024):
        """
        Upload a large file to Dropbox in chunks.

        This helper function handles splitting the file content into chunks
        and performing a session-based upload to Dropbox, suitable for files
        larger than the direct upload limit (150MB).

        Parameters
        ----------
        content : bytes
            The full byte content of the file to upload.
        full_dropbox_path : str
            The complete Dropbox path where the file will be saved (including filename).
        chunk_size : int, optional
            The size (in bytes) of each upload chunk. Default is 25MB (25 * 1024 * 1024).

        Returns
        -------
        None
            The file is uploaded to Dropbox upon successful completion.
        """
        upload_session_start_result = self.dbx.files_upload_session_start(content[:chunk_size])
        cursor = dropbox.files.UploadSessionCursor(
            session_id=upload_session_start_result.session_id, offset=chunk_size
        )
        remaining_content = content[chunk_size:]

        while len(remaining_content) > 0:
            if len(remaining_content) > chunk_size:
                self.dbx.files_upload_session_append_v2(
                    remaining_content[:chunk_size], cursor
                )
                cursor.offset += chunk_size
                remaining_content = remaining_content[chunk_size:]
            else:
                self.dbx.files_upload_session_finish(
                    remaining_content,
                    cursor,
                    dropbox.files.CommitInfo(path=full_dropbox_path, mode=dropbox.files.WriteMode.overwrite),
                )
                remaining_content = b""

    def _initialize_paths(self, input_path: str, output_path: str):
        """
        Initialize Dropbox folders for raw, clean, and output data.

        Parameters
        ----------
        input_path : str
            The base path in Dropbox where the input data is stored.
        output_path : str
            The base path in Dropbox where the output data will be stored.

        Returns
        -------
        raw_input_path : str
            Path of the Dropbox folder containing the raw data.
        clean_input_path : str
            Path of the Dropbox folder containing the clean data.
        output_path : str
            Path of the Dropbox folder where outputs will be saved.
        """
        raw_input_path = f"{input_path}/raw"
        clean_input_path = f"{input_path}/clean"

        for path in [raw_input_path, clean_input_path, output_path]:
            if not self.folder_exists(path):
                self.create_folder(path)

        return raw_input_path, clean_input_path, output_path

    def folder_exists(self, folder_path):
        """
        Check if a Dropbox folder exists.

        Parameters
        ----------
        folder_path : str
            Path of the folder in Dropbox to check.

        Returns
        -------
        exists : bool
            True if the folder exists, False if not.
        """
        try:
            self.dbx.files_get_metadata(folder_path)
            return True
        except dropbox.exceptions.ApiError as err:
            if isinstance(err.error, dropbox.files.GetMetadataError) and err.error.is_path() and \
               err.error.get_path().is_not_found():
                return False
            else:
                logging.error(f"Error checking existence of folder '{folder_path}': {err}")
                return False

    def create_folder(self, folder_path, return_path=False):
        """
        Create a folder in Dropbox if it does not exist.

        Parameters
        ----------
        folder_path : str
            Path of the folder to create in Dropbox.
        return_path : bool, optional
            If True, return the created folder path, by default False.

        Returns
        -------
        folder_path : str, optional
            The path of the created folder if return_path is True.
        """
        try:
            self.dbx.files_create_folder_v2(folder_path)
            logging.info(f"Folder '{folder_path}' created successfully.")
        except dropbox.exceptions.ApiError as err:
            if isinstance(err.error, dropbox.files.CreateFolderError) and err.error.is_path() and \
               err.error.get_path().is_conflict():
                logging.info(f"Folder '{folder_path}' already exists.")
            else:
                logging.error(f"Failed to create folder '{folder_path}': {err}")

        if return_path:
            return folder_path

    def list_files_in_folder(self, folder_path, recursive=False):
        """
        List file names in a Dropbox folder.

        Parameters
        ----------
        folder_path : str
            Path of the folder in Dropbox.
        recursive : bool, optional
            If True, list files recursively, by default False.

        Returns
        -------
        file_names : list of str
            List of file names in the specified folder.
        """
        try:
            result = self.dbx.files_list_folder(folder_path, recursive=recursive, limit=2000)
            return [entry.name for entry in result.entries]
        except dropbox.exceptions.ApiError as err:
            logging.error(f"Failed to list files in folder '{folder_path}': {err}")
            return []

    def list_files_with_relative_paths(self, *args, **kwargs):
        """
        List file paths relative to a specified Dropbox folder.

        All arguments are forwarded to `self.dbx.files_list_folder`.

        Parameters
        ----------
        *args
            Positional arguments for `files_list_folder`, first positional arg is folder path.
        **kwargs
            Keyword arguments for `files_list_folder`, e.g., path, recursive.

        Returns
        -------
        relative_paths : list of str
            Paths of files relative to the specified folder.
        """
        try:
            result = self.dbx.files_list_folder(*args, **kwargs)
            entries = result.entries
            while result.has_more:
                result = self.dbx.files_list_folder_continue(result.cursor)
                entries.extend(result.entries)

            folder_path = args[0] if args else kwargs.get('path', '')
            return [
                entry.path_display[len(folder_path):].lstrip('/')
                for entry in entries
                if isinstance(entry, dropbox.files.FileMetadata)
            ]
        except dropbox.exceptions.ApiError as err:
            logging.error(f"Error listing relative paths: {err}")
            return []

    def upload_file_directly(self, file_bytes: bytes,dbx_path: str,directory: str,filename: str, print_success=True):
        """
        Upload raw file bytes directly to Dropbox.

        Parameters
        ----------
        file_bytes : bytes
            Content of the file to upload.
        dbx_path : str
            Base Dropbox path where the file will be saved.
        directory : str
            Subdirectory within the base path for the file.
        filename : str
            Name of the file, e.g., 'data.csv'.
        log_success : bool, optional
            If True, log a success message, by default True.

        Raises
        ------
        ApiError
            If a Dropbox API error occurs during upload.
        """
        def uploader(content: bytes, full_path: str):
            # exactly the same Dropbox call you had before
            self.dbx.files_upload(
                content,
                full_path,
                mode=dropbox.files.WriteMode.overwrite,
                autorename=False
            )

        # _base_write will build the path, wrap try/except, and call our uploader
        self._base_write(
            content=file_bytes,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            print_success=print_success
        )

    def write_bytes(self, file_bytes, dbx_path: str, directory: str, filename: str, print_success=True):
        """
        Upload file bytes to Dropbox and optionally print status.

        Parameters
        ----------
        file_bytes : bytes
            File content to upload.
        dbx_path : str
            Base Dropbox path for the file.
        directory : str
            Subdirectory within the base path.
        filename : str
            Name of the file to save.
        print_success : bool, optional
            If True, print success message, by default True.
        """
        def uploader(content: bytes, full_path: str):
            self.dbx.files_upload(
                content,
                full_path,
                mode=dropbox.files.WriteMode.overwrite
            )
        
        self._base_write(
            content=file_bytes,
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            uploader=uploader,
            print_success=print_success
        )
    def download_file_directly(self, dbx_path: str, directory: str, filename: str) -> bytes:
        """
        Download raw file bytes directly from Dropbox.

        Parameters
        ----------
        dbx_path : str
            Base Dropbox path where the file is located.
        directory : str
            Subdirectory within the base path for the file.
        filename : str
            Name of the file to download.

        Returns
        -------
        bytes
            File content as bytes.

        Raises
        ------
        Exception
            If any error occurs during download.
        """
        full_path = self._construct_path(dbx_path, directory, filename)
        try:
            metadata, res = self.dbx.files_download(full_path)
            return res.content
        except Exception as e:
            print(f"Error downloading '{filename}' from Dropbox: {e}")
            return None
        
        