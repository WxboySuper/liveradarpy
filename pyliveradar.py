# pyliveradar
# This module handles live radar data acquisition and updates.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import logging
import json
from pathlib import Path
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Update the initialization of NEXRAD_SITES to handle missing or invalid nexrad_sites.json
NEXRAD_SITES = None
try:
    json_path = os.path.join(os.path.dirname(__file__), "nexrad_sites.json")
    with open(json_path, "r") as f:
        NEXRAD_SITES = json.load(f)
except FileNotFoundError:
    logging.error("nexrad_sites.json file not found.")
except json.JSONDecodeError:
    logging.error("nexrad_sites.json contains invalid JSON.")
except Exception as e:
    logging.error(f"Unexpected error loading NEXRAD sites: {e}")

class PyLiveRadar:
    def __init__(self):
        """Initialize the PyLiveRadar module."""
        # Initialize instance variables
        self._site_cache = None

    @staticmethod
    def _is_valid_nexrad_site(self, station: str) -> bool:
        """
        Check if the given station is a valid NEXRAD site.

        Args:
            station (str): The radar station identifier (e.g., KTLX).

        Returns:
            bool: True if the station is valid, False otherwise.
        """
        if not NEXRAD_SITES:
            logging.error("NEXRAD sites data is empty or not loaded.")
            return False
        if NEXRAD_SITES is None:
            logging.error("NEXRAD sites data is not loaded (None).")
            return False
        return any(site.get("id") == station for site in NEXRAD_SITES)

    def fetch_radar_data(self, station: str, output_dir: str):
        """
        Fetch radar data for a given station from the Unidata/UCAR L2 server.

        Args:
            station (str): The radar station identifier (e.g., KTLX).
            output_dir (str): The directory to save the downloaded radar data.

        Returns:
            str: The path to the downloaded radar data file.
        """
        # Validate output_dir
        output_dir_path = Path(output_dir)
        if not output_dir_path.exists() or not output_dir_path.is_dir():
            logging.error("Invalid output directory: %s. Ensure it exists and is a directory.", output_dir)
            return None

        if not self._is_valid_nexrad_site(station):
            logging.error("Invalid NEXRAD site: %s", station)
            return None

        base_url = "https://thredds.ucar.edu/thredds/fileServer/nexrad/level2"
        try:
            # Construct the URL for the radar station
            now = datetime.now(timezone.utc)  # Updated to use timezone-aware UTC
            date_path = now.strftime("%Y/%m/%d")
            url = f"{base_url}/{date_path}/{station}/"
            logging.debug("Constructed URL: %s", url)

            # Fetch the directory listing
            response = requests.get(url)
            logging.debug("HTTP GET Response Status Code: %d", response.status_code)
            response.raise_for_status()
            logging.debug("Fetched directory listing successfully.")

            # Parse the latest file (simplified for MVP)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")
            logging.debug("Found %d links in the directory listing.", len(links))
            if not links:
                raise ValueError("No radar data files found.")

            # Debug log the raw directory listing
            logging.debug("Raw directory listing: %s", [link['href'] for link in links])

            # Filter links to include only valid radar data files
            valid_extensions = [".ar2v"]  # Updated to include .ar2v as the valid extension
            valid_links = [link['href'] for link in links if 'href' in link.attrs and any(link['href'].endswith(ext) for ext in valid_extensions)]
            logging.debug("Filtered valid links: %s", valid_links)

            if not valid_links:
                raise ValueError("No valid radar data files found.")

            # Sort the valid links lexicographically and select the last one
            latest_file = sorted(valid_links)[-1]
            file_url = f"{url}{latest_file}"
            logging.debug("Latest valid file URL: %s", file_url)

            # Download the radar data file
            radar_response = requests.get(file_url, stream=True)
            logging.debug("HTTP GET Response Status Code for file: %d", radar_response.status_code)
            radar_response.raise_for_status()
            logging.debug("Downloaded radar data file successfully.")

            # Save the file locally
            output_path = output_dir_path / latest_file  # Use Path for file path construction
            with output_path.open("wb") as f:  # Use Path's open method
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.debug("Saved radar data file to: %s", output_path)

            return str(output_path)  # Return the string representation of the path

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 404:
                logging.error("HTTP 404 Not Found: The requested resource could not be found.")
            elif http_err.response.status_code == 500:
                logging.error("HTTP 500 Internal Server Error: The server encountered an error.")
            else:
                logging.error("HTTP error occurred: %s", http_err)
        except requests.exceptions.RequestException as req_err:
            logging.error("RequestException occurred: %s", req_err)
        except ValueError as val_err:
            logging.error("ValueError occurred: %s", val_err)
        except Exception as e:
            logging.error("Unexpected error occurred: %s", e)

        return None



if __name__ == "__main__":
    print('Placeholder for live radar data acquisition and updates.')
