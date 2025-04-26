import os
import tempfile
import geopandas as gpd
import dropbox
import io

class ShapefileMixin:
    """
    Mixin providing Shapefile read/write capabilities via CoreMixin helpers.

    Methods
    -------
    read_shp(dbx_path, directory) -> GeoDataFrame or None
        Download and load a shapefile from Dropbox.
    write_shp(gdf, dbx_path, directory, filename) -> None
        Save a GeoDataFrame as a shapefile and upload its components.
    """

    def read_shp(self,
                dbx_path: str,
                directory: str, 
                filename: str,
                **kwargs) -> gpd.GeoDataFrame | None:
        """
        Download and load a shapefile (with all its components) from Dropbox into a GeoDataFrame.
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
        Save a GeoDataFrame as a shapefile and upload its components to Dropbox.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            The GeoDataFrame to save.
        dbx_path : str
            Base Dropbox path where the files will be uploaded.
        directory : str
            Subdirectory within the base path for saving the shapefile.
        filename : str
            Base name (without extension) for the shapefile components.

        Returns
        -------
        None
            Components are uploaded; errors are printed to stdout.
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
                    chunked=False,
                    print_success=True
                )
