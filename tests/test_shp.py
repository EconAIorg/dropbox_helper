import pytest
import os
import geopandas as gpd
from tests.test_init import dropbox_test_folder
from tests.utils import generate_random_gdf

@pytest.mark.usefixtures("dropbox_test_folder")
class TestShapefileMixin:
    fname = 'small_shapefile'

    @pytest.mark.order(9)
    def test_shapefile_upload(self):
        # generate a small GeoDataFrame
        gdf = generate_random_gdf(size=10)
        # upload via ShapefileMixin
        self.dbx_helper.write_shp(gdf, self.output_path, self.dir, self.fname)

        # verify core shapefile components exist
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        # mandatory components
        expected_exts = ['.shp', '.shx', '.dbf']
        for ext in expected_exts:
            assert f"{self.fname}{ext}" in files, \
                f"{self.fname}{ext} not found in Dropbox folder!"

    @pytest.mark.order(10)
    def test_shapefile_download(self):
        # ensure files are present
        files = self.dbx_helper.list_files_in_folder(
            os.path.join(self.dbx_helper.output_path, self.dir)
        )
        assert f"{self.fname}.shp" in files, "Shapefile not found!"

        # download and read the shapefile
        downloaded = self.dbx_helper.read_shp(self.output_path, self.dir, self.fname + '.shp')
        assert isinstance(downloaded, gpd.GeoDataFrame), "Downloaded object is not a GeoDataFrame."
        assert not downloaded.empty, "Downloaded GeoDataFrame is empty!"

        # compare structure: columns and geometry type
        # regenerate original to compare schema
        original = generate_random_gdf(size=10)
        assert list(downloaded.columns) == list(original.columns), \
            "Column mismatch between uploaded and downloaded shapefile."
        assert downloaded.geometry.geom_type.equals(original.geometry.geom_type), \
            "Geometry types differ."
