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
        from importlib import resources
        with resources.open_text(__package__, "nexrad_sites.json") as f:
            return json.load(f)
    except FileNotFoundError as e:
        logger.error("nexrad_sites.json file not found.")
        raise FileNotFoundError(
            "nexrad_sites.json file is required but was not found."
        ) from e
    except json.JSONDecodeError as e:
        logger.error("nexrad_sites.json contains invalid JSON.")
        raise ValueError("nexrad_sites.json contains invalid JSON.") from e
    except Exception as e:
        logger.error("Unexpected error loading NEXRAD sites: %s", e)
        raise RuntimeError(
            "Unexpected error occurred while loading NEXRAD sites."
        ) from e


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
            self._site_cache = {
                site.get("id")
                for site in sites
                if site.get("id") is not None
            }
        return self._site_cache

    def _is_valid_nexrad_site(self, station: str) -> None:
        """
        Check if the given station is a valid NEXRAD site.

        Args:
            station (str): The radar station identifier (e.g., KTLX).

        Raises:
            ValueError: If the station is invalid.
        """
        station = station.upper()
        site_cache = self._get_site_cache()
        if station not in site_cache:
            logger.error("Invalid NEXRAD site: %s", station)
            raise ValueError(f"Invalid NEXRAD site: {station}")

    @staticmethod
    def _validate_output_dir(output_dir: str) -> Path:
        output_dir_path = Path(output_dir)
        if not output_dir_path.exists():
            raise FileNotFoundError(f"Output directory does not exist: {output_dir}")
        if not output_dir_path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {output_dir}")
        return output_dir_path

    @staticmethod
    def _construct_station_url(station: str) -> str:
        base_url = "https://thredds.ucar.edu/thredds/fileServer/nexrad/level2"
        now = datetime.now(timezone.utc)
        date_path = now.strftime("%Y/%m/%d")
        return f"{base_url}/{date_path}/{station}/"

    @staticmethod
    def _fetch_and_filter_links(url: str) -> list:
        headers = {"User-Agent": "PyLiveRadar/1.0"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a")
        if not links:
            raise ValueError("No radar data files found.")
        sanitized_links = [
            os.path.basename(link['href'])
            for link in links
            if 'href' in link.attrs
        ]
        valid_extensions = [".ar2v", ".ar2v.gz"]
        valid_links = [
            link
            for link in sanitized_links
            if any(link.endswith(ext) for ext in valid_extensions)
        ]
        if not valid_links:
            raise ValueError("No valid radar data files found.")
        return valid_links

    @staticmethod
    def _get_latest_file(valid_links: list) -> str:
        return sorted(valid_links)[-1]

    @staticmethod
    def _download_and_save_file(
        url: str,
        latest_file: str,
        output_dir_path: Path
    ) -> str:
        file_url = f"{url}{latest_file}"
        headers = {"User-Agent": "PyLiveRadar/1.0"}
        radar_response = requests.get(
            file_url, headers=headers, timeout=10, stream=True
        )
        radar_response.raise_for_status()
        temp_output_path = output_dir_path / f"{latest_file}.tmp"
        final_output_path = output_dir_path / latest_file
        try:
            with temp_output_path.open("wb") as f:
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)
            temp_output_path.replace(final_output_path)
        except OSError as e:
            if temp_output_path.exists():
                temp_output_path.unlink()
            logger.error("File operation failed: %s", e)
            raise
        except Exception as e:
            if temp_output_path.exists():
                temp_output_path.unlink()
            logger.error("Unexpected error during file download: %s", e)
            raise RuntimeError("Unexpected error occurred during file download.") from e
        return str(final_output_path)

    def fetch_radar_data(self, station: str, output_dir: str):
        """
        Downloads the latest radar data file for a specified station from the
        Unidata/UCAR L2 server.

        Fetches the most recent radar data for a valid NEXRAD station, saving the
        file to the given output directory. Returns the local file path if successful.

        Args:
            station: Radar station identifier (e.g., 'KTLX').
            output_dir: Directory where the downloaded radar data file will be saved.

        Returns:
            str: The path to the downloaded radar data file.

        Raises:
            FileNotFoundError: If the output directory does not exist.
            NotADirectoryError: If the output path is not a directory.
            ValueError: If the station is invalid or no valid radar data files are
                found.
            requests.exceptions.RequestException: If an HTTP request fails.
        """
        output_dir_path = self._validate_output_dir(output_dir)
        self._is_valid_nexrad_site(station)
        url = self._construct_station_url(station)
        valid_links = self._fetch_and_filter_links(url)
        latest_file = self._get_latest_file(valid_links)
        return self._download_and_save_file(url, latest_file, output_dir_path)
