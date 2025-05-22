# pyliveradar
# This module handles live radar data acquisition and updates.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import logging
import json
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PyLiveRadar:
    def __init__(self):
        """Initialize the PyLiveRadar module."""
        pass

    def _is_valid_nexrad_site(self, station: str) -> bool:
        """
        Check if the given station is a valid NEXRAD site.

        Args:
            station (str): The radar station identifier (e.g., KTLX).

        Returns:
            bool: True if the station is valid, False otherwise.
        """
        try:
            with open("nexrad_sites.json", "r") as f:
                nexrad_sites = json.load(f)
            return any(site["id"] == station for site in nexrad_sites)
        except Exception as e:
            logging.error(f"Error reading NEXRAD sites: {e}")
            return False

    def fetch_radar_data(self, station: str, output_dir: str):
        """
        Fetch radar data for a given station from the Unidata/UCAR L2 server.

        Args:
            station (str): The radar station identifier (e.g., KTLX).
            output_dir (str): The directory to save the downloaded radar data.

        Returns:
            str: The path to the downloaded radar data file.
        """
        if not self._is_valid_nexrad_site(station):
            logging.error(f"Invalid NEXRAD site: {station}")
            return None

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

            # Debug log the raw directory listing
            logging.debug(f"Raw directory listing: {[link['href'] for link in links]}")

            # Filter links to include only valid radar data files
            valid_extensions = [".gz", ".bz2"]
            valid_links = [link['href'] for link in links if 'href' in link.attrs and any(link['href'].endswith(ext) for ext in valid_extensions)]
            logging.debug(f"Filtered valid links: {valid_links}")

            if not valid_links:
                raise ValueError("No valid radar data files found.")

            # Sort the valid links lexicographically and select the last one
            latest_file = sorted(valid_links)[-1]
            file_url = f"{url}{latest_file}"
            logging.debug(f"Latest valid file URL: {file_url}")

            # Download the radar data file
            radar_response = requests.get(file_url, stream=True)
            logging.debug(f"HTTP GET Response Status Code for file: {radar_response.status_code}")
            radar_response.raise_for_status()
            logging.debug("Downloaded radar data file successfully.")

            # Convert output_dir to a Path object
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists

            # Save the file locally
            output_path = output_dir / latest_file  # Use Path for file path construction
            with output_path.open("wb") as f:  # Use Path's open method
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.debug(f"Saved radar data file to: {output_path}")

            return str(output_path)  # Return the string representation of the path

        except requests.exceptions.RequestException as req_err:
            logging.error(f"RequestException occurred: {req_err}")
        except ValueError as val_err:
            logging.error(f"ValueError occurred: {val_err}")
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")

        return None



if __name__ == "__main__":
    print('Placeholder for live radar data acquisition and updates.')
