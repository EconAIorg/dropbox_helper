from .core_mixin import CoreMixin
from .csv_mixin import CSVMixin
from .parquet_mixin import ParquetMixin
from .pickle_mixin import PickleMixin
from .shapefile_mixin import ShapefileMixin
# from .npz_mixin import NPZMixin
# from .raster_mixin import RasterMixin
# from .json_mixin import JSONMixin
# from .report_mixin import ReportMixin
import os
from dotenv import load_dotenv
load_dotenv()

# __all__ = ["DropboxHelper", "get_dbx_helper"]

class DropboxHelper(CoreMixin, CSVMixin, ParquetMixin, PickleMixin, ShapefileMixin): # , , NPZMixin, RasterMixin, JSONMixin, ReportMixin):
    """
    Class for interfacing with Dropbox.

    Parameters
    ----------
    dbx_token : str
        The Dropbox API token.
    dbx_key : str
        The Dropbox app key.
    dbx_secret : str
        The Dropbox app secret.
    input_path : str
        The base path in Dropbox where the input data is stored.
    output_path : str
        The base path in Dropbox where the output data will be stored.
    custom_paths : bool, optional
        If True, the input_path and output_path will be used as is.
        If False, the input_path and output_path will be used to create the raw_input_path,
        clean_input_path and output_path.

    Attributes
    ----------
    dbx : dropbox.Dropbox
        The Dropbox object used to interact with the Dropbox API.
    raw_input_path : str
        The path of the Dropbox folder containing the raw data.
    clean_input_path : str
        The path of the Dropbox folder containing the clean data.
    output_path : str
        The path of the Dropbox folder where the output data will be saved.
    """
    pass

def get_dbx_helper(token = 'DROPBOX_TOKEN',key = 'DROPBOX_KEY', secret= 'DROPBOX_SECRET'):
    token = os.getenv(token)
    app_key = os.getenv(key)
    app_secret = os.getenv(secret)
    
    if not token or not app_key or not app_secret:
        raise ValueError("Missing Dropbox credentials in environment variables.")
    
    return DropboxHelper(dbx_token=token, dbx_key=app_key, dbx_secret=app_secret)

# dbx_helper = get_dbx_helper()