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
                 filename:str,
                 **kwargs) -> gpd.GeoDataFrame | None:
        """
        Download and load a shapefile from Dropbox into a GeoDataFrame.

        Parameters
        ----------
        dbx_path : str
            Base Dropbox path where the shapefile components are stored.
        directory : str
            Subdirectory within the base path containing the shapefile (without leading slash).

        Returns
        -------
        geopandas.GeoDataFrame or None
            The loaded GeoDataFrame, or None if an error occurs.
        """
        def loader(content: bytes, **kwargs):
            return gpd.read_file(io.BytesIO(content), **kwargs)

        # downloader: full download via Dropbox SDK
        def downloader(full_path: str):
            return self.dbx.files_download(full_path)

        return self._base_read(
            dbx_path=dbx_path,
            directory=directory,
            filename=filename,
            downloader=downloader,
            loader=loader,
            **kwargs
        )


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
