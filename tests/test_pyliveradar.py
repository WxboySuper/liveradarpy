import os
import unittest
from unittest.mock import patch, MagicMock, mock_open, ANY
import requests
from pyliveradar import PyLiveRadar
import tempfile
import numpy as np


class TestPyLiveRadar(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.test_output_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        """Clean up the test environment."""
        self.test_output_dir.cleanup()

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data(self, mock_get):
        """
        Tests that fetch_radar_data downloads a radar file and saves it to the specified
        directory.

        Simulates HTTP responses for directory listing and file download, verifies that
        the downloaded file exists, and validates the requested URLs.
        """
        # Mock the response for the directory listing
        mock_response_dir = MagicMock()
        mock_response_dir.text = (
            "<html><body>"
            "<a href='file1.ar2v'>file1.ar2v</a>"
            "<a href='file2.ar2v'>file2.ar2v</a>"
            "</body></html>"
        )

        # Mock the response for the file download
        mock_response_file = MagicMock()
        mock_response_file.iter_content = lambda chunk_size: [b"data"]

        # Use side_effect to provide a sequence of responses
        mock_get.side_effect = [mock_response_dir, mock_response_file]

        # Create an instance of PyLiveRadar
        radar = PyLiveRadar()

        # Define test parameters
        station = "KTLX"

        # Call the function
        result = radar.fetch_radar_data(station, self.test_output_dir.name)

        # Assertions
        self.assertIsNotNone(result)
        self.assertTrue(os.path.exists(result))

        # Validate the requested URLs
        # skipcq: PYL-W0212
        expected_dir_url = radar._construct_station_url(station)
        expected_file_url = f"{expected_dir_url}file2.ar2v"
        mock_get.assert_any_call(expected_dir_url, headers=ANY, timeout=10)
        mock_get.assert_any_call(
            expected_file_url, headers=ANY, timeout=10, stream=True
        )

    @patch("pyliveradar.pyart")
    @patch("pyliveradar.rasterio")
    @patch("pyliveradar.Path")
    def test_process_radar_to_raster_file_not_found(self, mock_path, mock_rasterio, mock_pyart):
        """Test process_radar_to_raster with non-existent radar file."""
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = False
        mock_path.return_value = mock_path_instance
        
        radar = PyLiveRadar()
        with self.assertRaises(FileNotFoundError) as context:
            radar.process_radar_to_raster("nonexistent.ar2v", "output.tif")
        self.assertIn("Radar file not found", str(context.exception))

    @patch("pyliveradar.pyart")
    @patch("pyliveradar.rasterio")
    @patch("pyliveradar.Path")
    def test_process_radar_to_raster_invalid_field(self, mock_path, mock_rasterio, mock_pyart):
        """Test process_radar_to_raster with invalid field name."""
        # Setup mocks
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path.return_value = mock_path_instance
        
        mock_radar = MagicMock()
        mock_radar.fields = {'reflectivity': {}, 'velocity': {}}
        mock_pyart.io.read.return_value = mock_radar
        
        radar = PyLiveRadar()
        with self.assertRaises(ValueError) as context:
            radar.process_radar_to_raster("test.ar2v", "output.tif", field="invalid_field")
        self.assertIn("Field 'invalid_field' not available", str(context.exception))

    @patch("pyliveradar.pyart")
    @patch("pyliveradar.rasterio")
    @patch("pyliveradar.Path")
    @patch("pyliveradar.np")
    def test_process_radar_to_raster_success(self, mock_np, mock_path, mock_rasterio, mock_pyart):
        """Test successful radar processing to GeoTIFF."""
        # Setup path mocks
        mock_path_instance = MagicMock()
        mock_path_instance.exists.return_value = True
        mock_path_instance.name = "test.ar2v"
        mock_path_instance.stem = "test"
        mock_path_instance.parent.mkdir = MagicMock()
        mock_path.return_value = mock_path_instance
        
        # Setup radar mock
        mock_radar = MagicMock()
        mock_radar.fields = {'reflectivity': {}}
        mock_radar.nsweeps = 5
        mock_radar.latitude = {'data': [35.0]}
        mock_radar.longitude = {'data': [-97.0]}
        mock_radar.metadata = {'instrument_name': 'Test Radar'}
        mock_pyart.io.read.return_value = mock_radar
        
        # Setup grid mock
        mock_grid = MagicMock()
        mock_grid_data = MagicMock()
        mock_grid_data.filled.return_value = np.array([[1, 2], [3, 4]])  # Return numpy array
        mock_grid.fields = {'reflectivity': {'data': [mock_grid_data]}}
        mock_pyart.map.grid_from_radars.return_value = mock_grid
        
        # Setup numpy mock
        mock_np.nan = float('nan')
        
        # Setup rasterio mock
        mock_rasterio_context = MagicMock()
        mock_rasterio.open.return_value.__enter__ = MagicMock(return_value=mock_rasterio_context)
        mock_rasterio.open.return_value.__exit__ = MagicMock(return_value=None)
        
        radar = PyLiveRadar()
        result = radar.process_radar_to_raster("test.ar2v", "output.tif")
        
        # Assertions
        self.assertEqual(result, "output.tif")
        mock_pyart.io.read.assert_called_once()
        mock_pyart.map.grid_from_radars.assert_called_once()
        mock_rasterio.open.assert_called_once()

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='[{"id": "KTLX", "name": "Oklahoma City, OK"}]'
    )
    def test_is_valid_nexrad_site_valid(self, mock_file):
        """
        Tests that _is_valid_nexrad_site returns True for a valid NEXRAD station
        code.
        """
        radar = PyLiveRadar()
        # skipcq: PYL-W0212
        radar._is_valid_nexrad_site(
            "KTLX"
        )  # Ensure no exception is raised for valid site

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data='[{"id": "KTLX", "name": "Oklahoma City, OK"}]'
    )
    def test_is_valid_nexrad_site_invalid(self, mock_file):
        """
        Tests that _is_valid_nexrad_site returns False for an invalid radar station
        code.
        """
        radar = PyLiveRadar()
        with self.assertRaises(ValueError):
            # skipcq: PYL-W0212
            radar._is_valid_nexrad_site("INVALID")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_invalid_station(self, mock_get):
        """Test fetch_radar_data with an invalid station ID."""
        os.makedirs("test_output", exist_ok=True)
        radar = PyLiveRadar()
        with self.assertRaises(ValueError) as context:
            radar.fetch_radar_data("INVALID", "test_output")
        self.assertEqual(str(context.exception), "Invalid NEXRAD site: INVALID")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_http_error(self, mock_get):
        """Test fetch_radar_data with an HTTP error."""
        mock_get.return_value.status_code = 404
        http_error = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value.raise_for_status.side_effect = http_error
        radar = PyLiveRadar()
        with self.assertRaises(requests.exceptions.HTTPError) as context:
            radar.fetch_radar_data("KTLX", self.test_output_dir.name)
        self.assertEqual(str(context.exception), "404 Not Found")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_empty_directory(self, mock_get):
        """Test fetch_radar_data with an empty directory listing."""
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "<html></html>"
        radar = PyLiveRadar()
        with self.assertRaises(ValueError) as context:
            radar.fetch_radar_data("KTLX", self.test_output_dir.name)
        self.assertEqual(str(context.exception), "No radar data files found.")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_failed_download(self, mock_get):
        """Test fetch_radar_data with a failed file download."""
        mock_get.side_effect = requests.exceptions.RequestException("Download failed")
        radar = PyLiveRadar()
        with self.assertRaises(requests.exceptions.RequestException) as context:
            radar.fetch_radar_data("KTLX", self.test_output_dir.name)
        self.assertEqual(str(context.exception), "Download failed")


if __name__ == "__main__":
    unittest.main()
