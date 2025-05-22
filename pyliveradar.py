# pyliveradar
# This module handles live radar data acquisition and updates.

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import logging
import json
from pathlib import Path
import os
from functools import lru_cache

# Create a module-level logger
logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def _load_sites():
    """
    Load and parse the nexrad_sites.json file.

    Returns:
        list: Parsed JSON data from the nexrad_sites.json file.

    Raises:
        FileNotFoundError: If the nexrad_sites.json file is not found.
        ValueError: If the JSON is invalid.
    """
    try:
        json_path = os.path.join(os.path.dirname(__file__), "nexrad_sites.json")
        with open(json_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("nexrad_sites.json file not found.")
        raise FileNotFoundError("nexrad_sites.json file is required but was not found.")
    except json.JSONDecodeError:
        logger.error("nexrad_sites.json contains invalid JSON.")
        raise ValueError("nexrad_sites.json contains invalid JSON and cannot be loaded.")
    except Exception as e:
        logger.error("Unexpected error loading NEXRAD sites: %s", e)
        raise

class PyLiveRadar:
    def __init__(self):
        """Initialize the PyLiveRadar module."""
        # Initialize instance variables
        self._site_cache = None

    def _get_site_cache(self):
        """
        Load and cache the set of valid NEXRAD site IDs.

        Returns:
            set: A set of valid NEXRAD site IDs.
        """
        if self._site_cache is None:
            sites = _load_sites()
            self._site_cache = {site.get("id") for site in sites if site.get("id") is not None}
        return self._site_cache

    def _is_valid_nexrad_site(self, station: str) -> bool:
        """
        Check if the given station is a valid NEXRAD site.

        Args:
            station (str): The radar station identifier (e.g., KTLX).

        Returns:
            bool: True if the station is valid, False otherwise.

        Raises:
            ValueError: If the station is invalid.
        """
        site_cache = self._get_site_cache()
        if station not in site_cache:
            logger.error("Invalid NEXRAD site: %s", station)
            raise ValueError(f"Invalid NEXRAD site: {station}")
        return True

    def fetch_radar_data(self, station: str, output_dir: str):
        """
        Fetch radar data for a given station from the Unidata/UCAR L2 server.

        Args:
            station (str): The radar station identifier (e.g., KTLX).
            output_dir (str): The directory to save the downloaded radar data.

        Returns:
            str: The path to the downloaded radar data file.
        """
        logger.debug("test")
        # Validate output_dir
        output_dir_path = Path(output_dir)
        if not output_dir_path.exists():
            raise FileNotFoundError(f"Output directory does not exist: {output_dir}")
        if not output_dir_path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {output_dir}")

        # Validate the station
        self._is_valid_nexrad_site(station)

        base_url = "https://thredds.ucar.edu/thredds/fileServer/nexrad/level2"
        try:
            # Construct the URL for the radar station
            now = datetime.now(timezone.utc)  # Updated to use timezone-aware UTC
            date_path = now.strftime("%Y/%m/%d")
            url = f"{base_url}/{date_path}/{station}/"
            logger.debug("Constructed URL: %s", url)

            # Fetch the directory listing
            headers = {"User-Agent": "PyLiveRadar/1.0"}
            response = requests.get(url, headers=headers, timeout=10)
            logger.debug("HTTP GET Response Status Code: %d", response.status_code)
            response.raise_for_status()
            logger.debug("Fetched directory listing successfully.")

            # Parse the latest file (simplified for MVP)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")
            logger.debug("Found %d links in the directory listing.", len(links))
            if not links:
                raise ValueError("No radar data files found.")

            # Debug log the raw directory listing
            logger.debug("Raw directory listing: %s", [link['href'] for link in links])

            # Sanitize filenames to prevent path traversal
            sanitized_links = [os.path.basename(link['href']) for link in links if 'href' in link.attrs]

            # Update valid extensions to include '.ar2v.gz'
            valid_extensions = [".ar2v", ".ar2v.gz"]

            # Filter links to include only valid radar data files
            valid_links = [link for link in sanitized_links if any(link.endswith(ext) for ext in valid_extensions)]
            logger.debug("Filtered valid links: %s", valid_links)

            if not valid_links:
                raise ValueError("No valid radar data files found.")

            # Sort the valid links lexicographically and select the last one
            latest_file = sorted(valid_links)[-1]
            file_url = f"{url}{latest_file}"
            logger.debug("Latest valid file URL: %s", file_url)

            # Download the radar data file
            radar_response = requests.get(file_url, headers=headers, timeout=10, stream=True)
            logger.debug("HTTP GET Response Status Code for file: %d", radar_response.status_code)
            radar_response.raise_for_status()
            logger.debug("Downloaded radar data file successfully.")

            # Save the file locally using a temporary file
            temp_output_path = output_dir_path / f"{latest_file}.tmp"  # Temporary file path
            final_output_path = output_dir_path / latest_file  # Final output path
            with temp_output_path.open("wb") as f:  # Use Path's open method
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.debug("Saved radar data to temporary file: %s", temp_output_path)

            # Atomically move the temporary file to the final output path
            temp_output_path.rename(final_output_path)
            logger.debug("Renamed temporary file to final output path: %s", final_output_path)

            return str(final_output_path)  # Return the string representation of the path

        except requests.exceptions.HTTPError as http_err:
            if http_err.response is not None:
                if http_err.response.status_code == 404:
                    logger.error("HTTP 404 Not Found: The requested resource could not be found.")
                elif http_err.response.status_code == 500:
                    logger.error("HTTP 500 Internal Server Error: The server encountered an error.")
                else:
                    logger.error("HTTP error occurred: %s", http_err)
            else:
                logger.error("HTTP error occurred but response is None: %s", http_err)
            raise
        except requests.exceptions.RequestException as req_err:
            logger.error("RequestException occurred: %s", req_err)
            raise
        except ValueError as val_err:
            logger.error("ValueError occurred: %s", val_err)
            raise
        except Exception as e:
            logger.error("Unexpected error occurred: %s", e)
            raise



if __name__ == "__main__":
    print('Placeholder for live radar data acquisition and updates.')
