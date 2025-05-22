# pyliveradar
# This module handles live radar data acquisition and updates.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PyLiveRadar:
    def __init__(self):
        """Initialize the PyLiveRadar module."""
        pass

    def fetch_radar_data(self, station: str, output_dir: str):
        """
        Fetch radar data for a given station from the Unidata/UCAR L2 server.

        Args:
            station (str): The radar station identifier (e.g., KTLX).
            output_dir (str): The directory to save the downloaded radar data.

        Returns:
            str: The path to the downloaded radar data file.
        """
        base_url = "https://thredds.ucar.edu/thredds/fileServer/nexrad/level2"
        try:
            # Construct the URL for the radar station
            now = datetime.now(timezone.utc)  # Updated to use timezone-aware UTC
            date_path = now.strftime("%Y/%m/%d")
            url = f"{base_url}/{date_path}/{station}/"
            logging.debug(f"Constructed URL: {url}")

            # Fetch the directory listing
            response = requests.get(url)
            logging.debug(f"HTTP GET Response Status Code: {response.status_code}")
            response.raise_for_status()
            logging.debug("Fetched directory listing successfully.")

            # Parse the latest file (simplified for MVP)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")
            logging.debug(f"Found {len(links)} links in the directory listing.")
            if not links:
                raise ValueError("No radar data files found.")

            latest_file = links[-1]['href']
            file_url = f"{url}{latest_file}"
            logging.debug(f"Latest file URL: {file_url}")

            # Download the radar data file
            radar_response = requests.get(file_url, stream=True)
            logging.debug(f"HTTP GET Response Status Code for file: {radar_response.status_code}")
            radar_response.raise_for_status()
            logging.debug("Downloaded radar data file successfully.")

            # Save the file locally
            output_path = f"{output_dir}/{latest_file}"
            with open(output_path, "wb") as f:
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.debug(f"Saved radar data file to: {output_path}")

            return output_path

        except requests.exceptions.RequestException as req_err:
            logging.error(f"RequestException occurred: {req_err}")
        except ValueError as val_err:
            logging.error(f"ValueError occurred: {val_err}")
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")

        return None



if __name__ == "__main__":
    print('Placeholder for live radar data acquisition and updates.')
