# Tasks for pyliveradar

## Core Functionalities

### Data Acquisition
- Implement HTTP/FTP client to fetch radar data from servers (e.g., NEXRAD L2, L3, MRMS).
- Support multiple servers with fallback mechanisms.

### Data Processing
- Decode radar data formats (e.g., NEXRAD Level 2/3).
- Convert radar data into raster formats (e.g., GeoTIFF, NetCDF).

### Data Delivery
- Save processed data in user-specified formats.
- Provide a simple API or CLI for user interaction.

---

## Supporting Features

### Error Handling
- Handle server unavailability, timeouts, and data corruption gracefully.

### Configuration
- Allow users to configure:
  - Server preferences and fallbacks.
  - Output file format and directory.

### Logging
- Log acquisition, processing, and delivery steps for debugging and transparency.

### Testing
- Include unit tests for acquisition, processing, and delivery.

---

## MVP Plan
1. Focus on NEXRAD L2:
   - Fetch data from a single server.
   - Decode and process it into a raster format (e.g., GeoTIFF).
   - Save the file locally.

2. Expand to Multiple Servers:
   - Add fallback mechanisms for server unavailability.

3. Add User Options:
   - Allow users to specify output format and directory.
