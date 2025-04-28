import os
import tempfile
import geopandas as gpd
import dropbox
import io

class ShapefileMixin:
    """
    Mixin providing Shapefile read/write capabilities via CoreMixin helpers.

    This class assumes the presence of `self.dbx` (a Dropbox client) and `_base_write`
    for file handling.

    Methods
    -------
    read_shp(dbx_path, directory, filename, **kwargs)
        Download and load a shapefile (with its components) from Dropbox into a GeoDataFrame.

    write_shp(gdf, dbx_path, directory, filename)
        Save a GeoDataFrame to a shapefile and upload its components to Dropbox.
    """

    def read_shp(self,
                dbx_path: str,
                directory: str, 
                filename: str,
                **kwargs) -> gpd.GeoDataFrame | None:
        """
        Download and load a shapefile (with all its components) from Dropbox into a GeoDataFrame.

        Parameters
        ----------
        dbx_path : str
            Base Dropbox path where the shapefile components are located.
        directory : str
            Subdirectory within the base path containing the shapefile components.
        filename : str
            Name of the main shapefile (.shp extension required).
        **kwargs : dict, optional
            Additional keyword arguments passed to `geopandas.read_file`.

        Returns
        -------
        geopandas.GeoDataFrame or None
            The loaded GeoDataFrame if successful; otherwise, None.
        """
        # Needed shapefile extensions
        SHP_EXTENSIONS = [".shp", ".shx", ".dbf", ".prj", ".cpg"]

        try:
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download all components
                for ext in SHP_EXTENSIONS:
                    full_path = os.path.join(dbx_path, directory, filename.replace(".shp", ext))
                    try:
                        md, res = self.dbx.files_download(full_path)
                        local_fp = os.path.join(tmpdir, os.path.basename(full_path))
                        with open(local_fp, "wb") as f:
                            f.write(res.content)
                    except dropbox.exceptions.ApiError as e:
                        # Allow missing .prj or .cpg files
                        if ext in [".prj", ".cpg"]:
                            continue
                        else:
                            raise e

                # Now read the .shp file from local temp directory
                shp_path = os.path.join(tmpdir, filename)
                return gpd.read_file(shp_path, **kwargs)

        except Exception as e:
            print(f"Failed to read shapefile: {e}")
            return None


    def write_shp(self,
                  gdf: gpd.GeoDataFrame,
                  dbx_path: str,
                  directory: str,
                  filename: str) -> None:
        """
        Save a GeoDataFrame to a shapefile and upload all component files to Dropbox.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            The GeoDataFrame to serialize and save.
        dbx_path : str
            Base Dropbox path where the shapefile components will be uploaded.
        directory : str
            Subdirectory within the base path where files will be saved.
        filename : str
            Base filename (without extension) for the shapefile (e.g., 'my_shapefile').

        Returns
        -------
        None
            The method uploads components individually; upload success is printed.
        """
        exts = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
        with tempfile.TemporaryDirectory() as tmpdir:
            gdf.to_file(os.path.join(tmpdir, filename + '.shp'), driver='ESRI Shapefile')
            for ext in exts:
                local = os.path.join(tmpdir, filename + ext)
                if not os.path.exists(local):
                    continue
                content = open(local, 'rb').read()
                self._base_write(
                    content=content,
                    dbx_path=dbx_path,
                    directory=directory,
                    filename=filename + ext,
                    uploader=lambda b, p: self.dbx.files_upload(
                        b, p, mode=dropbox.files.WriteMode.overwrite
                    ),
                    print_success=True
                )
