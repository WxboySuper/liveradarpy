import os
import shutil
import unittest
from unittest.mock import patch, MagicMock, mock_open
import requests
from pyliveradar import PyLiveRadar


class TestPyLiveRadar(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.test_output_dir = "test_output"
        if not os.path.exists(self.test_output_dir):
            os.makedirs(self.test_output_dir)

    def tearDown(self):
        """Clean up the test environment."""
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data(self, mock_get):
        """
        Tests that fetch_radar_data downloads a radar file and saves it to the specified
        directory.

        Simulates HTTP responses for directory listing and file download, verifies that
        the downloaded file exists, and cleans up created files and directories after
        the test.
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
        shutil.rmtree(output_dir)

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
        self.assertTrue(radar._is_valid_nexrad_site("KTLX"))

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
            radar.fetch_radar_data("KTLX", self.test_output_dir)
        self.assertEqual(str(context.exception), "404 Not Found")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_empty_directory(self, mock_get):
        """Test fetch_radar_data with an empty directory listing."""
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "<html></html>"
        radar = PyLiveRadar()
        with self.assertRaises(ValueError) as context:
            radar.fetch_radar_data("KTLX", self.test_output_dir)
        self.assertEqual(str(context.exception), "No radar data files found.")

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data_failed_download(self, mock_get):
        """Test fetch_radar_data with a failed file download."""
        mock_get.side_effect = requests.exceptions.RequestException("Download failed")
        radar = PyLiveRadar()
        with self.assertRaises(requests.exceptions.RequestException) as context:
            radar.fetch_radar_data("KTLX", self.test_output_dir)
        self.assertEqual(str(context.exception), "Download failed")


if __name__ == "__main__":
    unittest.main()
