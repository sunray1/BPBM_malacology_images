import os
import json
import mysql.connector
from mysql.connector.plugins import mysql_native_password
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import unicodedata
import re

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="script.log",
    filemode="a"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)

# Load configuration
try:
    with open("config.json") as config_file:
        config = json.load(config_file)
except Exception as e:
    logging.critical("Failed to load config.json")
    logging.critical(e)
    raise

def get_db_connection():
    """Establish a database connection."""
    try:
        db_config = config["database"]
        conn = mysql.connector.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        logging.info("PILSBRy Database connection successful.")
        return conn
    except mysql.connector.Error as e:
        logging.critical("PILSBRy Database connection failed!")
        logging.critical(e)
        raise

def get_google_service():
    """Authenticate with Google Sheets API."""
    try:
        credentials_path = config["google_credentials"]
        with open(credentials_path) as source:
            info = json.load(source)

        credentials = service_account.Credentials.from_service_account_info(info)
        service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        logging.info("Google Sheets connection successful.")
        return service
    except Exception as e:
        logging.critical("Google Sheets connection failed!")
        logging.critical(e)
        raise

def create_folder(path):
    """Create a folder if it doesn't exist."""
    try:
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Created folder: {path}")
    except Exception as e:
        logging.error(f"Failed to create folder {path}")
        logging.error(e)

def move_files(source, destination):
    """Move files from source to destination."""
    try:
        logging.info(f"Moving files from {source} to {destination}")
        os.system(f"mv {source} {destination}")
    except Exception as e:
        logging.error(f"Error moving files from {source} to {destination}")
        logging.error(e)

def copy_files(source, destination):
    """Copy files from source to destination."""
    try:
        logging.info(f"Copying files from {source} to {destination}")
        os.system(f"cp {source} {destination}")
    except Exception as e:
        logging.error(f"Error copying files from {source} to {destination}")
        logging.error(e)

def delete_from_staging_sheet(rownum):
    """Remove a specific row from the staging sheet in Google Sheets."""
    try:
        service = get_google_service()
        spreadsheet_id = config["google_sheets"]["staging_spreadsheet_id"]
        sheet_id = config["google_sheets"]["staging_sheet_id"]

        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": rownum,
                        "endIndex": rownum + 1
                    }
                }
            }
        ]

        body = {"requests": requests}
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        logging.info(f"Successfully deleted record from the staging sheet.")
        rownum -= 1

    except Exception as e:
        logging.error(f"Failed to delete record from the staging sheet.")
        logging.error(e)
        
    return(rownum)

def delete_folder(path):
    """Delete a folder only if it is empty."""
    try:
        if os.path.exists(path):
            if not os.listdir(path):
                os.rmdir(path)
                logging.info(f"Deleted folder: {path}")
                return True
            else:
                logging.warning(f"Folder {path} is not empty, skipping deletion.")
                return False
        else:
            logging.warning(f"Folder {path} does not exist.")
            return False
    except Exception as e:
        logging.error(f"Failed to delete folder {path}")
        logging.error(e)
        return False

def check_folder_exists(staging_folder, foldername):
    """Check if the folder exists in the staging directory."""
    folder_path = os.path.join(staging_folder, foldername)

    if not os.path.exists(folder_path):
        logging.error(f"Skipping '{foldername}': Folder does not exist in the staging directory.")
        return False
    
    return True

def get_achat_subfamily(sciname):
    """Determine the Achatinellidae subfamily based on the genus."""
    subfamilies = {
        "Achatinellinae": ["Achatinella", "Newcombia", "Partulina", "Perdicella"],
        "Auriculellinae": ["Auriculella", "Gulickia"],
        "Tornatellidinae": ["Philopoa", "Tornatellaria", "Tornatellides"],
        "Tornatellininae": ["Elasmias"],
        "Pacificellinae": ["Lamellidea", "Pacificella", "Tornatellinops"]
    }
    for subfamily, genera in subfamilies.items():
        if any(genus in sciname for genus in genera):
            return subfamily
    return None

def clean_folder_name(name):
    """Removes diacriticals, replaces spaces with underscores, 
    and keeps only alphanumeric characters plus underscores."""
    if not name:
        return None
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    name = name.replace(" ", "_")
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)

    return name

def get_taxonomy_hierarchy(taxon, taxonrank, cursor):
    """Fetch full taxonomy hierarchy for a given taxon and rank."""
    tax_dic = {}

    # Try searching for taxon as is
    try:
        tax_dic[taxonrank] = taxon
        cursor.execute("SELECT tid FROM taxa WHERE sciname = %s;", (taxon,))
        tid = cursor.fetchone()['tid']
    except:
        tid = None

    # Try to search using Genus
    if taxonrank in ["Species", "Subspecies"] and not tid:
        try:
            genus = taxon.split(" ")[0]
            tax_dic["Genus"] = genus
            cursor.execute("SELECT tid FROM taxa WHERE sciname = %s;", (genus,))
            tid = cursor.fetchone()['tid']
        except:
            tid = None

    # Lookup in omoccurrences if taxon rank is below Family
    if taxonrank in ["Genus", "Species", "Subspecies", "Subfamily", "Family"] and not tid:
        try:
            cursor.execute("SELECT family FROM omoccurrences WHERE sciname = %s;", (taxon,))
            family = cursor.fetchone()['family']
            tax_dic["Family"] = family

            cursor.execute("SELECT tid FROM taxa WHERE sciname = %s;", (family,))
            tid = cursor.fetchone()['tid']
        except:
            tid = None

    # Retrieve parent taxonomy hierarchy from taxaenumtree
    if tid:
        cursor.execute("""
            SELECT t.rankid, t.sciname, tu.rankname
            FROM pilsbry.taxaenumtree te
            JOIN taxa t ON te.parenttid = t.tid
            JOIN taxonunits tu ON t.rankid = tu.rankid
            WHERE te.tid = %s
            ORDER BY te.parenttid DESC;
        """, (tid,))
        for row in cursor.fetchall():
            if row['rankname'] not in tax_dic:
                tax_dic[row['rankname']] = row['sciname']
    
    return tax_dic if len(tax_dic) > 2 else None
