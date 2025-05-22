import os
import unittest
from unittest.mock import patch, MagicMock, mock_open
import requests
from pyliveradar import PyLiveRadar

class TestPyLiveRadar(unittest.TestCase):

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data(self, mock_get):
        """Test the fetch_radar_data function."""
        # Mock the response for the directory listing
        mock_response_dir = MagicMock()
        mock_response_dir.text = """<html><body><a href='file1.ar2v'>file1.ar2v</a><a href='file2.ar2v'>file2.ar2v</a></body></html>"""
        mock_get.return_value = mock_response_dir

        # Mock the response for the file download
        mock_response_file = MagicMock()
        mock_response_file.iter_content = lambda chunk_size: [b"data"]
        mock_get.side_effect = [mock_response_dir, mock_response_file]

        # Create an instance of PyLiveRadar
        radar = PyLiveRadar()

        # Define test parameters
        station = "KTLX"
        output_dir = "test_output"

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Call the function
        result = radar.fetch_radar_data(station, output_dir)

        # Assertions
        self.assertIsNotNone(result)
        self.assertTrue(os.path.exists(result))

        # Clean up
        if os.path.exists(result):
            os.remove(result)
        os.rmdir(output_dir)

    @patch("builtins.open", new_callable=mock_open, read_data='[{"id": "KTLX", "name": "Oklahoma City, OK"}]')
    def test_is_valid_nexrad_site_valid(self, mock_file):
        """Test is_valid_nexrad_site with a valid station."""
        radar = PyLiveRadar()
        self.assertTrue(radar._is_valid_nexrad_site("KTLX"))

    @patch("builtins.open", new_callable=mock_open, read_data='[{"id": "KTLX", "name": "Oklahoma City, OK"}]')
    def test_is_valid_nexrad_site_invalid(self, mock_file):
        """Test is_valid_nexrad_site with an invalid station."""
        radar = PyLiveRadar()
        with self.assertRaises(ValueError):
            radar._is_valid_nexrad_site("INVALID")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_invalid_station(self, mock_get):
        """Test fetch_radar_data with an invalid station ID."""
        radar = PyLiveRadar()
        result = radar.fetch_radar_data("INVALID", "test_output")
        self.assertIsNone(result)

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_http_error(self, mock_get):
        """Test fetch_radar_data with an HTTP error."""
        mock_get.side_effect = requests.exceptions.HTTPError(response=MagicMock(status_code=404))
        radar = PyLiveRadar()
        result = radar.fetch_radar_data("KTLX", "test_output")
        self.assertIsNone(result)

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_empty_directory(self, mock_get):
        """Test fetch_radar_data with an empty directory listing."""
        mock_response = MagicMock()
        mock_response.text = "<html><body></body></html>"
        mock_get.return_value = mock_response
        radar = PyLiveRadar()
        result = radar.fetch_radar_data("KTLX", "test_output")
        self.assertIsNone(result)

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_failed_download(self, mock_get):
        """Test fetch_radar_data with a failed file download."""
        # Mock the response for the directory listing
        mock_response_dir = MagicMock()
        mock_response_dir.text = "<html><body><a href='file1.ar2v'>file1.ar2v</a></body></html>"
        # Mock the response for the file download to raise an exception
        mock_response_file = MagicMock()
        mock_response_file.raise_for_status.side_effect = requests.exceptions.RequestException("Download failed")
        mock_get.side_effect = [mock_response_dir, mock_response_file]

        radar = PyLiveRadar()
        result = radar.fetch_radar_data("KTLX", "test_output")
        self.assertIsNone(result)

    @patch("pyliveradar.PyLiveRadar._is_valid_nexrad_site", side_effect=ValueError("Invalid NEXRAD site"))
    def test_fetch_radar_data_invalid_station_raises_value_error(self, mock_is_valid):
        """Test fetch_radar_data raises ValueError for invalid station IDs."""
        radar = PyLiveRadar()
        # Ensure the output directory exists
        os.makedirs("test_output", exist_ok=True)
        with self.assertRaises(ValueError):
            radar.fetch_radar_data("INVALID", "test_output")

if __name__ == "__main__":
    unittest.main()
