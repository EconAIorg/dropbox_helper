# import dropbox
# import pandas as pd
# import io
# import pickle
# import os
# from dotenv import load_dotenv
# import requests
# import tempfile
# import geopandas as gpd
# from scipy.sparse import save_npz, load_npz
# import json

# class DropboxHelper:
#     """
#     Helper functions for reading and writing files to Dropbox.

#     Parameters
#     ----------
#     dbx_token : str
#         The Dropbox API token.
#     dbx_key : str
#         The Dropbox app key.
#     dbx_secret : str
#         The Dropbox app secret.
#     input_path : str
#         The base path in Dropbox where the input data is stored.
#     output_path : str
#         The base path in Dropbox where the output data will be stored.
#     custom_paths : bool, optional
#         If True, the input_path and output_path will be used as is.
#         If False, the input_path and output_path will be used to create the raw_input_path,
#         clean_input_path and output_path.

#     Attributes
#     ----------
#     dbx : dropbox.Dropbox
#         The Dropbox object used to interact with the Dropbox API.
#     raw_input_path : str
#         The path of the Dropbox folder containing the raw data.
#     clean_input_path : str
#         The path of the Dropbox folder containing the clean data.
#     output_path : str
#         The path of the Dropbox folder where the output data will be saved.
#     """

#     def __init__(self, dbx_token: str, dbx_key: str, dbx_secret: str,
#                  input_path: str, output_path: str, custom_paths: bool = False):
#         """
#         Initialize a new DropboxHelper instance.

#         Parameters
#         ----------
#         dbx_token : str
#             The Dropbox API token.
#         dbx_key : str
#             The Dropbox app key.
#         dbx_secret : str
#             The Dropbox app secret.
#         input_path : str
#             The base path in Dropbox where the input data is stored.
#         output_path : str
#             The base path in Dropbox where the output data will be stored.
#         custom_paths : bool, optional
#             If True, the input_path and output_path will be used as is.
#             If False, the input_path and output_path will be used to create the raw_input_path,
#             clean_input_path and output_path.
#         """
#         self.dbx = dropbox.Dropbox(oauth2_refresh_token=dbx_token,
#                                      app_key=dbx_key,
#                                      app_secret=dbx_secret)

#         if custom_paths:
#             self.input_path = input_path
#             self.output_path = output_path
#         else:
#             self.raw_input_path, self.clean_input_path, self.output_path = \
#                 self._initialize_paths(input_path, output_path)

#     def _initialize_paths(self, input_path: str, output_path: str):
#         """
#         Initialize Dropbox folder paths and create them if they do not exist.

#         Parameters
#         ----------
#         input_path : str
#             The base path in Dropbox where the input data is stored.
#         output_path : str
#             The base path in Dropbox where the output data will be stored.

#         Returns
#         -------
#         raw_input_path : str
#             The path of the Dropbox folder containing the raw data.
#         clean_input_path : str
#             The path of the Dropbox folder containing the clean data.
#         output_path : str
#             The path of the Dropbox folder where outputs will be saved.
#         """
#         raw_input_path = f"{input_path}/raw"
#         clean_input_path = f"{input_path}/clean"

#         for path in [raw_input_path, clean_input_path, output_path]:
#             if not self.folder_exists(path):
#                 self.create_folder(path)

#         return raw_input_path, clean_input_path, output_path

#     def folder_exists(self, folder_path: str) -> bool:
#         """
#         Check if a folder exists in Dropbox.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder in Dropbox to check.

#         Returns
#         -------
#         bool
#             True if the folder exists; False otherwise.
#         """
#         try:
#             self.dbx.files_get_metadata(folder_path)
#             return True
#         except dropbox.exceptions.ApiError as err:
#             if isinstance(err.error, dropbox.files.GetMetadataError) and \
#                err.error.is_path() and \
#                err.error.get_path().is_not_found():
#                 return False
#             print(f"Failed to check if folder '{folder_path}' exists: {err}")
#             return False

#     def create_folder(self, folder_path: str, return_path: bool = False) -> str:
#         """
#         Create a new folder in Dropbox if it does not already exist.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder to create in Dropbox.
#         return_path : bool, optional
#             If True, return the folder path after creation.

#         Returns
#         -------
#         str
#             The path of the created folder if return_path is True; otherwise None.
#         """
#         try:
#             self.dbx.files_create_folder_v2(folder_path)
#             print(f"Folder '{folder_path}' created successfully.")
#         except dropbox.exceptions.ApiError as err:
#             if isinstance(err.error, dropbox.files.CreateFolderError) and \
#                err.error.is_path() and \
#                err.error.get_path().is_conflict():
#                 print(f"Folder '{folder_path}' already exists.")
#             else:
#                 print(f"Failed to create folder '{folder_path}': {err}")

#         if return_path:
#             return folder_path

#     def list_files_in_folder(self, folder_path: str, recursive: bool = False) -> list:
#         """
#         List names of files in a Dropbox folder.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder in Dropbox.
#         recursive : bool, optional
#             If True, list files recursively.

#         Returns
#         -------
#         list of str
#             List of file names in the specified folder.
#         """
#         try:
#             files = self.dbx.files_list_folder(folder_path,
#                                                recursive=recursive,
#                                                limit=2000)
#             print(f"Files in folder '{folder_path}':")
#             return [f.name for f in files.entries]
#         except dropbox.exceptions.ApiError as err:
#             print(f"Failed to list files in folder '{folder_path}': {err}")
#             return []

#     def list_files_with_relative_paths(self, *args, **kwargs) -> list:
#         """
#         List files in a Dropbox folder and return their paths relative to that folder.

#         Parameters
#         ----------
#         *args, **kwargs
#             Arguments forwarded to self.dbx.files_list_folder.

#         Returns
#         -------
#         list of str
#             List of file paths relative to the base folder.
#         """
#         try:
#             result = self.dbx.files_list_folder(*args, **kwargs)
#             entries = result.entries
#             while result.has_more:
#                 result = self.dbx.files_list_folder_continue(result.cursor)
#                 entries.extend(result.entries)

#             folder_path = args[0] if args else kwargs.get('path', '')
#             relative_paths = [
#                 entry.path_display[len(folder_path):].lstrip('/')
#                 for entry in entries
#                 if isinstance(entry, dropbox.files.FileMetadata)
#             ]
#             return relative_paths
#         except dropbox.exceptions.ApiError as err:
#             print(f"API error: {err}")
#             return []

#     def read_csv(self, dbx_path: str, directory: str, filename: str,
#                  skiprows=None, header='infer', usecols=None,
#                  sep=',', index_col=None, parse_dates=None,
#                  dtype=None, mb_to_load=None) -> pd.DataFrame:
#         """
#         Read a CSV file from Dropbox into a pandas DataFrame.

#         Parameters
#         ----------
#         dbx_path : str
#             The base Dropbox path where the file is stored.
#         directory : str
#             The directory within the base path.
#         filename : str
#             The name of the CSV file.
#         skiprows : int or list, optional
#             Rows to skip at the beginning.
#         header : int, list of int, 'infer', optional
#             Row number(s) to use as the column names.
#         usecols : list, optional
#             Columns to read.
#         sep : str, optional
#             Field delimiter (default is ',').
#         index_col : int or sequence, optional
#             Column(s) to set as index.
#         parse_dates : bool or list, optional
#             Columns to parse as dates.
#         dtype : type or dict, optional
#             Data type for data or columns.
#         mb_to_load : float, optional
#             Number of megabytes to load (partial).

#         Returns
#         -------
#         pd.DataFrame
#             DataFrame with the CSV contents, or None if an error occurs.
#         """
#         if directory is None:
#             file_path = f'{dbx_path}/{filename}'
#         else:
#             file_path = f'{dbx_path}/{directory}/{filename}'
#         try:
#             if mb_to_load is None:
#                 _, res = self.dbx.files_download(file_path)
#                 df = pd.read_csv(io.BytesIO(res.content),
#                                  encoding='utf-8',
#                                  skiprows=skiprows,
#                                  header=header,
#                                  usecols=usecols,
#                                  sep=sep,
#                                  index_col=index_col,
#                                  parse_dates=parse_dates,
#                                  dtype=dtype)
#             else:
#                 metadata = self.dbx.files_get_temporary_link(file_path)
#                 temp_link = metadata.link
#                 headers = {"Range": f"bytes=0-{mb_to_load*1_048_576}"}
#                 res = requests.get(temp_link, headers=headers)
#                 if res.status_code != 206:
#                     print(f"Error: Received status code {res.status_code}")
#                     return None
#                 content_str = res.content.decode('utf-8')
#                 lines = content_str.splitlines()[:-1]
#                 df = pd.read_csv(io.StringIO("\n".join(lines)),
#                                  skiprows=skiprows,
#                                  header=header,
#                                  usecols=usecols,
#                                  sep=sep,
#                                  index_col=index_col,
#                                  parse_dates=parse_dates,
#                                  dtype=dtype)
#             return df
#         except Exception as e:
#             print(f"Error: {e}")
#             return None

#     def read_pickle(self, dbx_path: str, directory: str, filename: str):
#         """
#         Download and deserialize a pickle file from Dropbox.

#         Parameters
#         ----------
#         dbx_path : str
#             The base Dropbox path where the file is stored.
#         directory : str
#             The directory within the base path.
#         filename : str
#             Name of the pickle file (e.g., 'my_object.pkl').

#         Returns
#         -------
#         Any
#             The deserialized Python object, or None if loading fails.
#         """
#         file_path = f'{dbx_path}/{directory}/{filename}'
#         try:
#             _, res = self.dbx.files_download(file_path)
#             obj = pickle.loads(res.content)
#             return obj
#         except Exception as e:
#             print(f"Failed to load '{filename}' from Dropbox. Error: {e}")
#             return None

#     import dropbox
# import pandas as pd
# import io
# import pickle
# import os
# from dotenv import load_dotenv
# import requests
# tempfile
# import geopandas as gpd
# import logging
# from scipy.sparse import save_npz, load_npz
# import json
# gzip

# class DropboxHelper:
#     """
#     Helper functions for reading and writing files to Dropbox.

#     Parameters
#     ----------
#     dbx_token : str
#         The Dropbox API token.
#     dbx_key : str
#         The Dropbox app key.
#     dbx_secret : str
#         The Dropbox app secret.
#     input_path : str
#         The base path in Dropbox where the input data is stored.
#     output_path : str
#         The base path in Dropbox where the output data will be stored.
#     custom_paths : bool, optional
#         If True, the input_path and output_path will be used as is.
#         If False, the input_path and output_path will be used to create the raw_input_path,
#         clean_input_path and output_path.

#     Attributes
#     ----------
#     dbx : dropbox.Dropbox
#         The Dropbox object used to interact with the Dropbox API.
#     raw_input_path : str
#         The path of the Dropbox folder containing the raw data.
#     clean_input_path : str
#         The path of the Dropbox folder containing the clean data.
#     output_path : str
#         The path of the Dropbox folder where the output data will be saved.
#     """

#     def __init__(self, dbx_token: str, dbx_key: str, dbx_secret: str,
#                  input_path: str, output_path: str, custom_paths: bool = False):
#         """
#         Initialize a new DropboxHelper instance.

#         Parameters
#         ----------
#         dbx_token : str
#             The Dropbox API token.
#         dbx_key : str
#             The Dropbox app key.
#         dbx_secret : str
#             The Dropbox app secret.
#         input_path : str
#             The base path in Dropbox where the input data is stored.
#         output_path : str
#             The base path in Dropbox where the output data will be stored.
#         custom_paths : bool, optional
#             If True, the input_path and output_path will be used as is.
#             If False, the input_path and output_path will be used to create the raw_input_path,
#             clean_input_path and output_path.
#         """
#         self.dbx = dropbox.Dropbox(oauth2_refresh_token=dbx_token,
#                                      app_key=dbx_key,
#                                      app_secret=dbx_secret)

#         if custom_paths:
#             self.input_path = input_path
#             self.output_path = output_path
#         else:
#             self.raw_input_path, self.clean_input_path, self.output_path = \
#                 self._initialize_paths(input_path, output_path)

#     def _initialize_paths(self, input_path: str, output_path: str):
#         """
#         Initialize Dropbox folder paths and create them if they do not exist.

#         Parameters
#         ----------
#         input_path : str
#             The base path in Dropbox where the input data is stored.
#         output_path : str
#             The base path in Dropbox where the output data will be stored.

#         Returns
#         -------
#         raw_input_path : str
#             The path of the Dropbox folder containing the raw data.
#         clean_input_path : str
#             The path of the Dropbox folder containing the clean data.
#         output_path : str
#             The path of the Dropbox folder where outputs will be saved.
#         """
#         raw_input_path = f"{input_path}/raw"
#         clean_input_path = f"{input_path}/clean"

#         for path in [raw_input_path, clean_input_path, output_path]:
#             if not self.folder_exists(path):
#                 self.create_folder(path)

#         return raw_input_path, clean_input_path, output_path

#     def folder_exists(self, folder_path: str) -> bool:
#         """
#         Check if a folder exists in Dropbox.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder in Dropbox to check.

#         Returns
#         -------
#         bool
#             True if the folder exists; False otherwise.
#         """
#         try:
#             self.dbx.files_get_metadata(folder_path)
#             return True
#         except dropbox.exceptions.ApiError as err:
#             if isinstance(err.error, dropbox.files.GetMetadataError) and \
#                err.error.is_path() and \
#                err.error.get_path().is_not_found():
#                 return False
#             print(f"Failed to check if folder '{folder_path}' exists: {err}")
#             return False

#     def create_folder(self, folder_path: str, return_path: bool = False) -> str:
#         """
#         Create a new folder in Dropbox if it does not already exist.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder to create in Dropbox.
#         return_path : bool, optional
#             If True, return the folder path after creation.

#         Returns
#         -------
#         str
#             The path of the created folder if return_path is True; otherwise None.
#         """
#         try:
#             self.dbx.files_create_folder_v2(folder_path)
#             print(f"Folder '{folder_path}' created successfully.")
#         except dropbox.exceptions.ApiError as err:
#             if isinstance(err.error, dropbox.files.CreateFolderError) and \
#                err.error.is_path() and \
#                err.error.get_path().is_conflict():
#                 print(f"Folder '{folder_path}' already exists.")
#             else:
#                 print(f"Failed to create folder '{folder_path}': {err}")

#         if return_path:
#             return folder_path

#     def list_files_in_folder(self, folder_path: str, recursive: bool = False) -> list:
#         """
#         List names of files in a Dropbox folder.

#         Parameters
#         ----------
#         folder_path : str
#             The path of the folder in Dropbox.
#         recursive : bool, optional
#             If True, list files recursively.

#         Returns
#         -------
#         list of str
#             List of file names in the specified folder.
#         """
#         try:
#             files = self.dbx.files_list_folder(folder_path,
#                                                recursive=recursive,
#                                                limit=2000)
#             print(f"Files in folder '{folder_path}':")
#             return [f.name for f in files.entries]
#         except dropbox.exceptions.ApiError as err:
#             print(f"Failed to list files in folder '{folder_path}': {err}")
#             return []

#     def list_files_with_relative_paths(self, *args, **kwargs) -> list:
#         """
#         List files in a Dropbox folder and return their paths relative to that folder.

#         Parameters
#         ----------
#         *args, **kwargs
#             Arguments forwarded to self.dbx.files_list_folder.

#         Returns
#         -------
#         list of str
#             List of file paths relative to the base folder.
#         """
#         try:
#             result = self.dbx.files_list_folder(*args, **kwargs)
#             entries = result.entries
#             while result.has_more:
#                 result = self.dbx.files_list_folder_continue(result.cursor)
#                 entries.extend(result.entries)

#             folder_path = args[0] if args else kwargs.get('path', '')
#             relative_paths = [
#                 entry.path_display[len(folder_path):].lstrip('/')
#                 for entry in entries
#                 if isinstance(entry, dropbox.files.FileMetadata)
#             ]
#             return relative_paths
#         except dropbox.exceptions.ApiError as err:
#             print(f"API error: {err}")
#             return []

#     def read_csv(self, dbx_path: str, directory: str, filename: str,
#                  skiprows=None, header='infer', usecols=None,
#                  sep=',', index_col=None, parse_dates=None,
#                  dtype=None, mb_to_load=None) -> pd.DataFrame:
#         """
#         Read a CSV file from Dropbox into a pandas DataFrame.

#         Parameters
#         ----------
#         dbx_path : str
#             The base Dropbox path where the file is stored.
#         directory : str
#             The directory within the base path.
#         filename : str
#             The name of the CSV file.
#         skiprows : int or list, optional
#             Rows to skip at the beginning.
#         header : int, list of int, 'infer', optional
#             Row number(s) to use as the column names.
#         usecols : list, optional
#             Columns to read.
#         sep : str, optional
#             Field delimiter (default is ',').
#         index_col : int or sequence, optional
#             Column(s) to set as index.
#         parse_dates : bool or list, optional
#             Columns to parse as dates.
#         dtype : type or dict, optional
#             Data type for data or columns.
#         mb_to_load : float, optional
#             Number of megabytes to load (partial).

#         Returns
#         -------
#         pd.DataFrame
#             DataFrame with the CSV contents, or None if an error occurs.
#         """
#         if directory is None:
#             file_path = f'{dbx_path}/{filename}'
#         else:
#             file_path = f'{dbx_path}/{directory}/{filename}'
#         try:
#             if mb_to_load is None:
#                 _, res = self.dbx.files_download(file_path)
#                 df = pd.read_csv(io.BytesIO(res.content),
#                                  encoding='utf-8',
#                                  skiprows=skiprows,
#                                  header=header,
#                                  usecols=usecols,
#                                  sep=sep,
#                                  index_col=index_col,
#                                  parse_dates=parse_dates,
#                                  dtype=dtype)
#             else:
#                 metadata = self.dbx.files_get_temporary_link(file_path)
#                 temp_link = metadata.link
#                 headers = {"Range": f"bytes=0-{mb_to_load*1_048_576}"}
#                 res = requests.get(temp_link, headers=headers)
#                 if res.status_code != 206:
#                     print(f"Error: Received status code {res.status_code}")
#                     return None
#                 content_str = res.content.decode('utf-8')
#                 lines = content_str.splitlines()[:-1]
#                 df = pd.read_csv(io.StringIO("\n".join(lines)),
#                                  skiprows=skiprows,
#                                  header=header,
#                                  usecols=usecols,
#                                  sep=sep,
#                                  index_col=index_col,
#                                  parse_dates=parse_dates,
#                                  dtype=dtype)
#             return df
#         except Exception as e:
#             print(f"Error: {e}")
#             return None

#     def read_pickle(self, dbx_path: str, directory: str, filename: str):
#         """
#         Download and deserialize a pickle file from Dropbox.

#         Parameters
#         ----------
#         dbx_path : str
#             The base Dropbox path where the file is stored.
#         directory : str
#             The directory within the base path.
#         filename : str
#             Name of the pickle file (e.g., 'my_object.pkl').

#         Returns
#         -------
#         Any
#             The deserialized Python object, or None if loading fails.
#         """
#         file_path = f'{dbx_path}/{directory}/{filename}'
#         try:
#             _, res = self.dbx.files_download(file_path)
#             obj = pickle.loads(res.content)
#             return obj
#         except Exception as e:
#             print(f"Failed to load '{filename}' from Dropbox. Error: {e}")
#             return None

#     def read_shp(self, dbx_path: str, directory: str) -> gpd.GeoDataFrame:
#         """
#         Download and load a shapefile from Dropbox into a GeoDataFrame.

#         Parameters
#         ----------
#         dbx_path : str
#             The base Dropbox path where the files are stored.
#         directory : str
#             The directory containing shapefile components.

#         Returns
#         -------
#         gpd.GeoDataFrame
#             The loaded GeoDataFrame, or None if loading fails.
#         """
#         dir_path = f'{dbx_path}/{directory}'
#         try:
#             files = self.dbx.files_list_folder(dir_path).entries
#             shapefile_exts = {'.shp', '.shx', '.dbf', '.prj', '.cpg'}
#             shapefile_files = [f for f in files if any(f.name.endswith(ext)
#                                     for ext in shapefile_exts)]
#             if not shapefile_files:
#                 print(f"No shapefile components found in: {dir_path}")
#                 return None
#             with tempfile.TemporaryDirectory() as tmpdir:
#                 for file in shapefile_files:
#                     file_path = os.path.join(dir_path, file.name)
#                     _, res = self.dbx.files_download(file_path)
#                     temp_file_path = os.path.join(tmpdir, file.name)
#                     with open(temp_file_path, 'wb') as tmp:
#                         tmp.write(res.content)
#                 shp_file = next(f for f in shapefile_files if f.name.endswith('.shp'))
#                 data = gpd.read_file(os.path.join(tmpdir, shp_file.name))
#                 return data
#         except Exception as e:
#             print(f"Failed to load shapefile from Dropbox. Error: {e}")
#             return None

#     def read_tif(self, dbx_path: str, directory: str, filename: str) -> io.BytesIO:
#         """
#         Download a TIFF file from Dropbox and return as BytesIO.

#         Parameters
#         ----------
#         dbx_path : str
#             Base Dropbox path where the file is stored.
#         directory : str
#             Directory within the base path.
#         filename : str
#             Name of the TIFF file.

#         Returns
#         -------
#         io.BytesIO
#             In-memory raster file, or None if loading fails.
#         """
#         file_path = f'{dbx_path}/{directory}/{filename}'
#         try:
#             _, res = self.dbx.files_download(file_path)
#             return io.BytesIO(res.content)
#         except Exception as e:
#             print(f"Failed to load '{filename}' from Dropbox. Error: {e}")
#             return None

#     def read_npz(self, dbx_path: str, directory: str, filename: str):
#         """
#         Download and load an NPZ file from Dropbox as a sparse matrix.

#         Parameters
#         ----------
#         dbx_path : str
#             Base Dropbox path where the file is stored.
#         directory : str
#             Directory within the base path.
#         filename : str
#             Name of the .npz file.

#         Returns
#         -------
#         scipy.sparse.spmatrix
#             Loaded sparse matrix, or None if loading fails.
#         """
#         file_path = f"{dbx_path}/{directory}/{filename}"
