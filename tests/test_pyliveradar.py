import os
import unittest
from unittest.mock import patch, MagicMock
from pyliveradar import PyLiveRadar

class TestPyLiveRadar(unittest.TestCase):

    @patch("pyliveradar.requests.get")
    def test_fetch_radar_data(self, mock_get):
        """Test the fetch_radar_data function."""
        # Mock the response for the directory listing
        mock_response_dir = MagicMock()
        mock_response_dir.text = """<html><body><a href='file1'>file1</a><a href='file2'>file2</a></body></html>"""
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

if __name__ == "__main__":
    unittest.main()
