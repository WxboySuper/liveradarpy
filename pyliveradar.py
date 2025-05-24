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
import numpy as np
import pyart
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS

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
        # Try the modern approach first (Python 3.12+)
        try:
            from importlib import resources
            resource_path = resources.files("pyliveradar").joinpath("nexrad_sites.json")
            with resource_path.open("r") as f:
                return json.load(f)
        except (AttributeError, TypeError):
            # Fallback for older Python versions (3.9-3.11)
            try:
                from importlib import resources
                with resources.open_text("pyliveradar", "nexrad_sites.json") as f:
                    return json.load(f)
            except (FileNotFoundError, ModuleNotFoundError, TypeError):
                # Final fallback: use file system path relative to this module
                module_dir = os.path.dirname(__file__)
                json_path = os.path.join(module_dir, "nexrad_sites.json")
                with open(json_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
    except (OSError, UnicodeDecodeError) as e:
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
        try:
            radar_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error occurred: %s", e)
            raise requests.exceptions.HTTPError(
                f"HTTP error occurred while accessing {file_url}: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.error("Request error occurred: %s", e)
            raise requests.exceptions.RequestException(
                f"Request error occurred while accessing {file_url}: {e}") from e

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

    @staticmethod
    def _validate_input_file(radar_file_path):
        radar_path = Path(radar_file_path)
        if not radar_path.exists():
            raise FileNotFoundError(f"Radar file not found: {radar_file_path}")
        return radar_path

    @staticmethod
    def _prepare_output_path(output_path):
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    @staticmethod
    def _validate_grid_params(grid_resolution, grid_shape):
        if not (isinstance(grid_resolution, (int, float)) and grid_resolution > 0):
            raise ValueError(
                f"grid_resolution must be a positive number, got {grid_resolution!r}"
            )
        if (
            not isinstance(grid_shape, tuple)
            or len(grid_shape) != 2
            or not all(isinstance(x, int) and x > 0 for x in grid_shape)
        ):
            raise ValueError(
                (
                    f"grid_shape must be a tuple of two positive integers, "
                    f"got {grid_shape!r}"
                )
            )

    @staticmethod
    def _read_and_validate_radar(radar_path, field, sweep):
        logger.info("Reading radar data from: %s", radar_path)
        radar = pyart.io.read(str(radar_path))
        if field not in radar.fields:
            available_fields = list(radar.fields.keys())
            raise ValueError(
                f"Field '{field}' not available in radar data. "
                f"Available fields: {available_fields}"
            )
        if sweep >= radar.nsweeps:
            raise ValueError(
                (
                    f"Sweep {sweep} not available. "
                    f"Radar has {radar.nsweeps} sweeps (0-{radar.nsweeps-1})"
                )
            )
        return radar

    @staticmethod
    def _create_grid(
        radar,
        field,
        grid_resolution,
        grid_shape,
        h_factor=1.0,
        nb_factor=1.0,
        bsp=1.0,
        min_radius=250.0,
        weighting_function=None
    ):
        """
        Create a Cartesian grid from radar data using Py-ART.

        Args:
            radar: Py-ART radar object.
            field (str): Radar field to grid.
            grid_resolution (float): Grid resolution in meters.
            grid_shape (tuple): Grid dimensions (ny, nx).
            h_factor (float, optional): Horizontal smoothing factor. Default is 1.0.
                See Py-ART's grid_from_radars for details.
            nb_factor (float, optional): Barnes distance weighting factor.
                Default is 1.0.
            bsp (float, optional): Barnes smoothing parameter. Default is 1.0.
            min_radius (float, optional): Minimum radius for gridding (meters).
                Default is 250.0.
            weighting_function (callable, optional): Custom weighting function for
                gridding. If None, uses Py-ART's default.

        Returns:
            tuple: (grid, max_range)
        """
        # The default values are chosen to provide reasonable smoothing and coverage
        # for most NEXRAD Level II data. For advanced use cases, see:
        # https://arm-doe.github.io/pyart/API/generated/pyart.map.grid_from_radars.html
        max_range = grid_resolution * max(grid_shape) / 2
        grid_limits = ((-max_range, max_range), (-max_range, max_range))
        grid = pyart.map.grid_from_radars(
            radar,
            grid_shape=grid_shape,
            grid_limits=grid_limits,
            fields=[field],
            gridding_algo='map_gates_to_grid',
            h_factor=h_factor,
            nb_factor=nb_factor,
            bsp=bsp,
            min_radius=min_radius,
            weighting_function=weighting_function
        )
        return grid, max_range

    @staticmethod
    def _calculate_geotransform(radar, grid_shape, max_range):
        radar_lat = radar.latitude['data'][0]
        radar_lon = radar.longitude['data'][0]
        earth_radius = 6378137.0  # meters (WGS84)
        meters_per_deg_lat = (2 * np.pi * earth_radius) / 360.0
        meters_per_deg_lon = (
            2 * np.pi * earth_radius * np.cos(np.deg2rad(radar_lat))
        ) / 360.0
        delta_lat = max_range / meters_per_deg_lat
        delta_lon = max_range / meters_per_deg_lon
        west = radar_lon - delta_lon
        east = radar_lon + delta_lon
        south = radar_lat - delta_lat
        north = radar_lat + delta_lat
        transform = from_bounds(
            west, south, east, north,
            grid_shape[1], grid_shape[0]
        )
        return transform, radar_lat, radar_lon

    @staticmethod
    def _extract_gridded_data(grid, field):
        field_dict = grid.fields[field]
        if 'data' not in field_dict:
            logger.error("'data' key not found in grid.fields['%s'].", field)
            raise RuntimeError(f"'data' key not found in grid.fields['{field}'].")
        gridded_data = field_dict['data']
        # Accept both numpy arrays and lists (for test mocks)
        if isinstance(gridded_data, list):
            if len(gridded_data) < 1:
                logger.error("No data found in grid.fields['%s']['data'].", field)
                raise RuntimeError(f"No data found in grid.fields['{field}']['data'].")
            gridded_data = gridded_data[0]
        elif hasattr(gridded_data, 'shape'):
            if gridded_data.shape[0] < 1:
                logger.error("No data found in grid.fields['%s']['data'].", field)
                raise RuntimeError(f"No data found in grid.fields['{field}']['data'].")
            gridded_data = gridded_data[0]
        else:
            logger.error(
                "Unexpected type for grid.fields['%s']['data']: %s",
                field, type(gridded_data)
            )
            raise RuntimeError(
                f"Unexpected type for grid.fields['{field}']['data']: "
                f"{type(gridded_data)}"
            )
        if hasattr(gridded_data, 'mask'):
            gridded_data = gridded_data.filled(np.nan)
        return gridded_data

    @staticmethod
    def _write_geotiff(
        output_path, gridded_data, transform, field, sweep, radar_lat,
        radar_lon, radar, grid_resolution, radar_path
    ):
        logger.info("Writing GeoTIFF to: %s", output_path)
        with rasterio.open(
            output_path,
            'w',
            driver='GTiff',
            height=gridded_data.shape[0],
            width=gridded_data.shape[1],
            count=1,
            dtype=gridded_data.dtype,
            crs=CRS.from_epsg(4326),  # WGS84
            transform=transform,
            compress='lzw',
            nodata=np.nan
        ) as dst:
            dst.write(gridded_data, 1)
            dst.update_tags(
                FIELD=field,
                SWEEP=str(sweep),
                RADAR_LAT=str(radar_lat),
                RADAR_LON=str(radar_lon),
                RADAR_NAME=radar.metadata.get('instrument_name', 'Unknown'),
                SOURCE_FILE=str(radar_path.name),
                GRID_RESOLUTION=str(grid_resolution),
                PROCESSING_TIME=datetime.now(timezone.utc).isoformat(),
                DESCRIPTION=f"Radar {field} data processed with PyLiveRadar"
            )
        logger.info("Successfully created GeoTIFF: %s", output_path)
        return str(output_path)

    @staticmethod
    def process_radar_to_raster(
        radar_file_path: str,
        output_path: str,
        field: str = 'reflectivity',
        sweep: int = 0,
        grid_resolution: float = 1000.0,
        grid_shape: tuple = (400, 400),
        h_factor: float = 1.0,
        nb_factor: float = 1.0,
        bsp: float = 1.0,
        min_radius: float = 250.0,
        weighting_function=None
    ):
        """
        Process radar data file using Py-ART and export to GeoTIFF raster format.

        Reads a radar data file, extracts the specified field, converts it to a
        Cartesian grid, and saves as a GeoTIFF file with proper geospatial metadata.

        Args:
            radar_file_path (str): Path to the radar data file (e.g., .ar2v file).
            output_path (str): Path where the output GeoTIFF file will be saved.
            field (str, optional): Radar field to process. Defaults to 'reflectivity'.
                Common options: 'reflectivity', 'velocity',
                'spectrum_width', 'differential_reflectivity'.
            sweep (int, optional): Radar sweep number to process.
                Defaults to 0 (lowest tilt).
            grid_resolution (float, optional): Grid resolution in meters.
                Defaults to 1000.0.
            grid_shape (tuple, optional): Grid dimensions (ny, nx).
                Defaults to (400, 400).
            h_factor (float, optional): Horizontal smoothing factor for gridding.
                Default is 1.0. See Py-ART's grid_from_radars for details.
            nb_factor (float, optional): Barnes distance weighting factor.
                Default is 1.0.
            bsp (float, optional): Barnes smoothing parameter. Default is 1.0.
            min_radius (float, optional): Minimum radius for gridding (meters).
                Default is 250.0.
            weighting_function (callable, optional): Custom weighting function for
                gridding. If None, uses Py-ART's default.

        Returns:
            str: Path to the created GeoTIFF file.

        Raises:
            FileNotFoundError: If the radar file does not exist.
            ValueError: If the specified field is not available in the radar data.
            RuntimeError: If processing fails due to data issues.

        Example:
            >>> radar = PyLiveRadar()
            >>> radar_file = radar.fetch_radar_data('KTLX', './data')
            >>> raster_file = radar.process_radar_to_raster(
            ...     radar_file,
            ...     './output/reflectivity.tif',
            ...     field='reflectivity',
            ...     h_factor=1.2,
            ...     nb_factor=1.0,
            ...     bsp=1.0,
            ...     min_radius=300.0
            ... )
        """
        try:
            radar_path = PyLiveRadar._validate_input_file(radar_file_path)
            output_path = PyLiveRadar._prepare_output_path(output_path)
            PyLiveRadar._validate_grid_params(grid_resolution, grid_shape)
            radar = PyLiveRadar._read_and_validate_radar(radar_path, field, sweep)
            grid, max_range = PyLiveRadar._create_grid(
                radar, field, grid_resolution, grid_shape,
                h_factor=h_factor, nb_factor=nb_factor, bsp=bsp,
                min_radius=min_radius, weighting_function=weighting_function
            )
            gridded_data = PyLiveRadar._extract_gridded_data(grid, field)
            transform, radar_lat, radar_lon = PyLiveRadar._calculate_geotransform(
                radar, grid_shape, max_range
            )
            return PyLiveRadar._write_geotiff(
                output_path,
                gridded_data,
                transform,
                field,
                sweep,
                radar_lat,
                radar_lon,
                radar,
                grid_resolution,
                radar_path
            )
        except (ValueError, FileNotFoundError):
            raise
        except Exception as e:
            logger.error("Failed to process radar data: %s", e)
            raise RuntimeError(f"Radar processing failed: {e}") from e

    def fetch_and_process_radar(
            self,
            station: str,
            output_dir: str,
            field: str = 'reflectivity',
            sweep: int = 0,
            grid_resolution: float = 1000.0,
            grid_shape: tuple = (400, 400)
    ):
        """
        Convenience method to fetch and process radar data in one step.

        Downloads the latest radar data for a station and immediately processes it
        to a GeoTIFF raster format.

        Args:
            station (str): Radar station identifier (e.g., 'KTLX').
            output_dir (str): Directory for output files.
            field (str, optional): Radar field to process. Defaults to 'reflectivity'.
            sweep (int, optional): Radar sweep number. Defaults to 0.
            grid_resolution (float, optional): Grid resolution in meters.
                Defaults to 1000.0.
            grid_shape (tuple, optional): Grid dimensions. Defaults to (400, 400).

        Returns:
            dict: Dictionary containing paths to both raw and processed files.
                {'raw_file': str, 'processed_file': str}

        Raises:
            Same exceptions as fetch_radar_data and process_radar_to_raster methods.

        Example:
            >>> radar = PyLiveRadar()
            >>> result = radar.fetch_and_process_radar('KTLX', './output')
            >>> print(f"Raw: {result['raw_file']}, "
            ...       f"Processed: {result['processed_file']}")
        """
        # Fetch the raw radar data
        raw_file = self.fetch_radar_data(station, output_dir)

        # Generate output filename for processed data
        raw_path = Path(raw_file)
        processed_filename = f"{raw_path.stem}_{field}_sweep{sweep}.tif"
        processed_path = Path(output_dir) / processed_filename

        # Process the data
        processed_file = self.process_radar_to_raster(
            raw_file,
            str(processed_path),
            field=field,
            sweep=sweep,
            grid_resolution=grid_resolution,
            grid_shape=grid_shape
        )

        return {
            'raw_file': raw_file,
            'processed_file': processed_file
        }
