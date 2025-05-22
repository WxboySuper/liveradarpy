# pyliveradar
# This module handles live radar data acquisition and updates.

import requests
from bs4 import BeautifulSoup
from datetime import datetime

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
            now = datetime.now(datetime.timezone.utc)
            date_path = now.strftime("%Y/%m/%d")
            url = f"{base_url}/{date_path}/{station}/"

            # Fetch the directory listing
            response = requests.get(url)
            response.raise_for_status()

            # Parse the latest file (simplified for MVP)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a")
            if not links:
                raise ValueError("No radar data files found.")

            latest_file = links[-1]['href']
            file_url = f"{url}{latest_file}"

            # Download the radar data file
            radar_response = requests.get(file_url, stream=True)
            radar_response.raise_for_status()

            # Save the file locally
            output_path = f"{output_dir}/{latest_file}"
            with open(output_path, "wb") as f:
                for chunk in radar_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return output_path

        except Exception as e:
            print(f"Error fetching radar data: {e}")
            return None



if __name__ == "__main__":
    print('Placeholder for live radar data acquisition and updates.')
