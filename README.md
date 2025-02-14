# BPBM Malacology Collection Image Organizer

## Overview

This script is designed to assist the **Bishop Museum's Malacology Department** in organizing their collection images efficiently. It automates the process of moving images from a staging folder to their appropriate locations based on metadata stored in a Google Sheet. It also uploads images to **PILSBRy.org** using **DigitalOcean Spaces** links as public URLs and updates a Google Sheet with relevant information.

## Requirements

### Dependencies (For Python Users)

Ensure you have the following Python packages installed:

```bash
pip install boto3 mysql-connector-python google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```

### Configuration

Create a `config.json` file in the project directory with the following structure:

```json
{
    "folders": {
        "staging_folder": "path/to/staging/folder",
        "base_folder": "path/to/base/folder"
    },
    "google_sheets": {
        "staging_spreadsheet_id": "your-google-sheets-id",
        "database_spreadsheet_id": "your-google-sheets-id"
    },
    "digital_ocean": {
        "REGION": "your-region",
        "SPACE_NAME": "your-space-name",
        "ACCESS_KEY": "your-access-key",
        "SECRET_KEY": "your-secret-key"
    }
}
```

## Usage

### Running the Script

#### Python Users:

1. Ensure that the **Google Sheets API**, **MySQL database**, and **DigitalOcean Spaces** are properly configured.
2. Place images in the designated staging folder.
3. Run the script:
   ```bash
   python script.py
   ```

#### Non-Python Users:

1. Download the **compiled executable (.exe)**.
2. Double-click to run the program—no Python installation required.

### What the Script Does

- Reads metadata from Google Sheets.
- Retrieves specimen details from the database.
- Organizes and moves images based on taxonomy and type.
- Uploads images to **DigitalOcean Spaces** and stores the public URLs.
- Updates **PILSBRy.org** with the new image URLs.
- Updates Google Sheets with processed information.

## Summary Output

At the end of execution, a summary report is displayed, including:

- Number of successfully moved folders
- Number of errors encountered
- Remaining unprocessed records in staging
