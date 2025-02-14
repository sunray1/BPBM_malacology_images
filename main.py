import os
import logging
import boto3
from botocore.exceptions import NoCredentialsError
from utils import (
    get_db_connection,
    get_google_service,
    create_folder,
    move_files,
    copy_files,
    delete_folder,
    check_folder_exists,
    delete_from_staging_sheet,
    get_achat_subfamily,
    clean_folder_name,
    get_taxonomy_hierarchy
)

moved_folders_count = 0
error_folders_count = 0
rownum = 1

# Load configuration
try:
    import json
    with open("config.json") as config_file:
        config = json.load(config_file)
except Exception as e:
    logging.critical("Error loading configuration")
    logging.critical(e)
    raise

staging_folder = config["folders"]["staging_folder"]
base_folder = config["folders"]["base_folder"]

# Connect to database
try:
    db_conn = get_db_connection()
    cursor = db_conn.cursor(dictionary=True)
except Exception as e:
    logging.critical("Failed to establish database connection!")
    raise

# Connect to Google Sheets
try:
    service = get_google_service()
    spreadsheet_id = config["google_sheets"]["staging_spreadsheet_id"]
except Exception as e:
    logging.critical("Failed to initialize Google Sheets API!")
    raise

# Connect to Digital Ocean
try:
    session = boto3.session.Session()
    client = session.client(
        "s3",
        region_name=config["digital_ocean"]["REGION"],
        endpoint_url=f"https://{config['digital_ocean']['REGION']}.digitaloceanspaces.com",
        aws_access_key_id=config["digital_ocean"]["ACCESS_KEY"],
        aws_secret_access_key=config["digital_ocean"]["SECRET_KEY"],
    )
    logging.info("Digial Ocean connection successful.")
except Exception as e:
    logging.critical("Failed to connect to Digital Ocean!")
    raise

def get_occid_by_specimen_number(specimennumber):
    """Fetch occid and specimenid from spcollectionspecimen using specimennumber.
       If multiple records exist, log an error and return None.
    """

    cursor.execute("SELECT COUNT(*) FROM pilsbry.spcollectionspecimen WHERE specimennumber = %s", (specimennumber,))
    count = cursor.fetchone()["COUNT(*)"]

    if count > 1:
        logging.error(f"Error searching for specimen number {specimennumber}: Multiple records found in database.")
        return None

    if count == 0:
        logging.warning(f"Error searching for specimen number {specimennumber}: No matching record found.")
        return None

    cursor.execute(
        "SELECT occid, specimenid FROM pilsbry.spcollectionspecimen WHERE specimennumber = %s", 
        (specimennumber,)
    )
    return cursor.fetchone()  # Return the single valid record

def get_specimen_info_by_occid(occid):
    """Fetch full specimen details from omoccurrences using occid."""
    cursor.execute(
        "SELECT occid, family, sciname, identificationQualifier, catalognumber, fieldnumber, "
        "country, stateProvince, island, locality, typestatus "
        "FROM omoccurrences WHERE occid = %s", 
        (occid,)
    )
    return cursor.fetchone()

def get_specimen_info_by_catalog_number(catalognumber):
    """Fetch specimen details by catalog number if specimen number is missing.
       If multiple records exist, log an error and return None.
    """
    cat_num = catalognumber.lstrip("BPBM")

    cursor.execute("SELECT COUNT(*) FROM omoccurrences WHERE collid = 1 AND catalognumber = %s", (cat_num,))
    count = cursor.fetchone()["COUNT(*)"]

    if count > 1:
        logging.error(f"Error searching for catalog number {catalognumber}: Multiple records found in database.")
        return None

    if count == 0:
        logging.warning(f"Error searching for catalog number {catalognumber}: No matching record found.")
        return None

    cursor.execute(
        "SELECT occid, family, sciname, identificationQualifier, catalognumber, fieldnumber, "
        "country, stateProvince, island, locality, typestatus "
        "FROM omoccurrences WHERE collid = 1 AND catalognumber = %s", 
        (cat_num,)
    )
    return cursor.fetchone()

def get_captive_info(boxnumber):
    """Fetch specimen details for Captive images using boxnumber.
       If multiple records exist, log an error and return None.
    """
    cursor.execute("SELECT COUNT(*) FROM omoccurrences WHERE boxnumber = %s", (boxnumber,))
    count = cursor.fetchone()["COUNT(*)"]

    if count > 1:
        logging.error(f"Error searching for box number {boxnumber}: Multiple records found in database.")
        return None

    if count == 0:
        logging.warning(f"Error searching for box number {boxnumber}: No matching record found.")
        return None

    cursor.execute(
        "SELECT occid, family, sciname, identificationQualifier, typestatus, fieldnumber, country, stateProvince, island, locality, boxnumber "
        "FROM omoccurrences WHERE boxnumber = %s", (boxnumber,)
    )
    return cursor.fetchone()  # Return the single valid record

def get_field_info(fieldnumber):
    """Fetch location details for Field type images using fieldnumber."""
    cursor.execute(
        "SELECT country, stateProvince, island, municipality, locality "
        "FROM omoccurrences WHERE fieldnumber = %s LIMIT 1", 
        (fieldnumber,)
    )
    return cursor.fetchone()

def extract_taxonomy_info(specimen_info):
    """Extracts family, genus_species, and subfamily from specimen data."""
    if not specimen_info:
        return None, None, None

    family = specimen_info["family"]
    sciname = specimen_info["sciname"].replace(" ", "_")
    subfamily = get_achat_subfamily(sciname) if family == "Achatinellidae" else None

    return family, sciname, subfamily

def add_to_google_sheets(
    occid=None, specimenid=None, phylum="Mollusca", taxaclass="Gastropoda",
    family=None, subfamily=None, sciname=None, identificationQualifier=None,
    catalognumber=None, specimennumber=None, imagetype=None, specimentype=None,
    typestatus=None, fieldnumber=None, captiverearingbox=None, country=None,
    state=None, island=None, locality=None, plated=None, folderpathway=None):
    """Append a new row of data to the Google Sheets database"""

    try:
        sciname = sciname.replace("_", " ") if sciname else ""
        identificationQualifier = identificationQualifier.replace("_", " ") if identificationQualifier else ""
        
        if folderpathway:
            folderpathway = folderpathway.replace("/mnt/d", "D:").replace("/", "\\")
        else:
            folderpathway = ""

        values = [
            occid or "", specimenid or "", phylum, taxaclass, family or "", subfamily or "",
            sciname or "", identificationQualifier or "", f"{catalognumber}" if catalognumber else "",
            specimennumber or "", imagetype or "", specimentype or "", typestatus or "",
            fieldnumber or "", captiverearingbox or "", country or "", state or "",
            island or "", locality or "", plated or "", folderpathway
        ]

        spreadsheet_id = config["google_sheets"]["database_spreadsheet_id"]
        range_name = "BPBM_Images_Database!A1:T1"
        body = {"values": [values]}

        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()

        logging.info(f"Successfully added record to Google Sheets Database")

    except Exception as e:
        logging.error(f"Failed to add record to Google Sheets Database")
        logging.error(e)

def add_to_digital_ocean(file_path, object_name):
    """Uploads an image to DigitalOcean Space"""
    try:
        client.upload_file(file_path, config["digital_ocean"]["SPACE_NAME"], object_name)
        print(f"Uploaded {file_path} to {config['digital_ocean']['SPACE_NAME']}/{object_name}")
        return f"https://{config['digital_ocean']['SPACE_NAME']}.{REGION}.digitaloceanspaces.com/{object_name}"
    except NoCredentialsError:
        print("Credentials not available")
        return None
    
def process_image_row(row):
    """Processes a single row from Google Sheets and moves images accordingly."""
    global moved_folders_count, error_folders_count, rownum

    try:
        foldername = row[11]
        imagetype = row[1]
        logging.info(f"Processing folder: {foldername} ({imagetype} Image)...")
        
        if not check_folder_exists(staging_folder, foldername):
            error_folders_count += 1
            return

        # Extract general fields
        specimennumber = row[2] if imagetype == "Type" else row[6] if imagetype == "Specimen" else None
        catalognumber = row[3] if imagetype == "Type" else row[7] if imagetype == "Specimen" else None
        plated = row[4] if imagetype == "Type" else row[8] if imagetype == "Specimen" else None
        boxnumber = row[5] if imagetype == "Captive" else None
        specimentype = row[9] if imagetype == "Specimen" else None
        fieldnumber = row[10] if imagetype == "Field" else None
        outreachduplicate = row[12] if imagetype == "Specimen" else None
        taxon = row[13] if imagetype == "Outreach" else row[15] if imagetype == "Non-Mollusk" else None
        taxonrank = row[14] if imagetype == "Outreach" else None
        phylum = row[16] if imagetype == "Non-Mollusk" else None if imagetype in ["Field", "Representative Plate"] else "Mollusca"
        identificationnumber = row[17] if imagetype == "Non-Mollusk" else None
        grouping = row[18] if imagetype == "Representative Plate" else None
        groupingfamily = row[19] if imagetype == "Representative Plate" else None
        groupinggeo = row[20] if imagetype == "Representative Plate" else None
        taxaclass = None
        occid = None
        specimenid = None
        family = None
        typestatus = None
        subfamily = None
        specimen_info = {}

        # Database lookups for relevant data
        if imagetype in ["Type", "Specimen", "Captive"]:
            specimen_info = None
    
            if specimennumber:
                specimen_data = get_occid_by_specimen_number(specimennumber)
                if specimen_data:
                    occid = specimen_data["occid"]
                    specimenid = specimen_data["specimenid"]
                    specimen_info = get_specimen_info_by_occid(occid)
    
                    if specimen_info and catalognumber:
                        if specimen_info["catalognumber"] and specimen_info["catalognumber"].strip() != catalognumber.strip().lstrip("BPBM"):
                            logging.warning(f"Skipping {foldername}: Catalog number mismatch (Expected: BPBM{specimen_info['catalognumber']}, Given: {catalognumber})")
                            error_folders_count += 1
                            return
            
            elif catalognumber:
                # Lookup by catalog number if specimen number is missing
                specimen_info = get_specimen_info_by_catalog_number(catalognumber)
    
            elif imagetype == "Captive" and boxnumber:
                # Lookup Captive specimens by box number
                specimen_info = get_captive_info(boxnumber)

            if not specimen_info:
                logging.warning(f"Skipping {foldername}: Cannot get record information from PILSBRy.")
                error_folders_count += 1
                return
            else:
                occid = specimen_info["occid"]
    
            # Extract taxonomy information
            family, sciname, subfamily = extract_taxonomy_info(specimen_info)

        elif imagetype == "Field":
            field_info = get_field_info(fieldnumber)
            
            if not field_info:
                logging.warning(f"Skipping {foldername}: No location data found for fieldnumber {fieldnumber}.")
                error_folders_count += 1
                return

        # Construct destination paths dynamically
        if imagetype in ["Type", "Specimen"]:
            id_qualifier = f"_{specimen_info['identificationQualifier'].replace(' ', '_').replace('.', '')}" if specimen_info["identificationQualifier"] else ""
            new_foldername = f"{sciname}{id_qualifier}_BPBM{specimen_info['catalognumber']}"
        
            if specimennumber:
                new_foldername += f"_{specimennumber}"
        
            if sciname == "Gastropoda":
                destination_path = os.path.join(base_folder, imagetype, "Gastropoda", new_foldername)
                taxaclass = "Gastropoda"
            elif sciname == "Bivalvia":
                destination_path = os.path.join(base_folder, imagetype, "Bivalvia", new_foldername)
                taxaclass = "Bivalvia"
            else:
                if not family:
                    logging.error(f"Skipping {new_foldername}: No family found in database record.")
                    return
                if subfamily:
                    destination_path = os.path.join(base_folder, imagetype, "Gastropoda", family, subfamily, new_foldername)
                    taxaclass = "Gastropoda"
                else:
                    destination_path = os.path.join(base_folder, imagetype, "Gastropoda", family, new_foldername)
                    taxaclass = "Gastropoda"
            if imagetype == "Specimen":
                if specimentype:
                    destination_path = os.path.join(destination_path, specimentype)

        elif imagetype == "Captive":
            taxaclass = "Gastropoda"
            if boxnumber:
                if subfamily:
                    destination_path = os.path.join(base_folder, imagetype, family, subfamily, sciname, f"Box_{boxnumber}")
                else:
                    destination_path = os.path.join(base_folder, imagetype, family, sciname, f"Box_{boxnumber}")
            else:
                logging.warning(f"Skipping {foldername}: Missing box number for Captive type.")
                return
        
        elif imagetype == "Field":
            country = clean_folder_name(field_info["country"])
            state = clean_folder_name(field_info["stateProvince"])
            island = clean_folder_name(field_info["island"])
            municipality = clean_folder_name(field_info["municipality"])
            locality = clean_folder_name(field_info["locality"])

            if not country or not state:
                logging.warning(f"Skipping {foldername}: Missing country or state for fieldnumber {fieldnumber}.")
                return

            if not locality and not municipality:
                logging.warning(f"Skipping {foldername}: Missing both locality and municipality for fieldnumber {fieldnumber}.")
                return

            location_folder = municipality if municipality else locality
            destination_path = os.path.join(base_folder, "Field", country, state)
            if island:
                destination_path = os.path.join(destination_path, island)
            destination_path = os.path.join(destination_path, location_folder, fieldnumber)
  
        elif imagetype == "Non-Mollusk":
            phylum = clean_folder_name(phylum)
            taxon = clean_folder_name(taxon)
            sciname = taxon
            identificationnumber = clean_folder_name(identificationnumber)

            if not phylum or not taxon:
                logging.warning(f"Skipping {foldername}: Missing required taxon or phylum for Non-Mollusk.")
                return
            
            pre_path = os.path.join(base_folder, "Non-Mollusk", phylum)
            if identificationnumber:
                destination_path = os.path.join(pre_path, f"{taxon}_{identificationnumber}")
            else:
                destination_path = os.path.join(pre_path, taxon)

        elif imagetype == "Outreach":
            if not taxon or not taxonrank:
                logging.warning(f"Skipping {foldername}: Missing taxon or taxon rank.")
                return

            # Retrieve taxonomy hierarchy
            taxonomy = get_taxonomy_hierarchy(taxon, taxonrank, cursor)
            if not taxonomy:
                logging.warning(f"Skipping {foldername}: Cannot find taxonomy for {taxon}.")
                return

            # Extract classification details
            taxaclass = taxonomy.get("Class", "")
            family = taxonomy.get("Family", "")

            # Construct folder structure
            destination_path = os.path.join(base_folder, "Outreach", taxaclass)
            if family:
                destination_path = os.path.join(destination_path, family)

            # Handle Achatinellidae separately
            if family == "Achatinellidae":
                subfamily = get_achat_subfamily(taxonomy.get("Genus", ""))
                if subfamily:
                    destination_path = os.path.join(destination_path, subfamily)

            destination_path = os.path.join(destination_path, clean_folder_name(taxon))        
        
        elif imagetype == "Representative Plate":
            groupingfamily = clean_folder_name(groupingfamily)
            groupinggeo = clean_folder_name(groupinggeo)

            if not groupingfamily and not groupinggeo:
                logging.warning(f"Skipping {foldername}: Missing required grouping for Representative Plate.")
                return
        
            if groupingfamily and groupinggeo:
                group_folder = f"{groupingfamily}_{groupinggeo}"
            else:
                group_folder = groupingfamily if groupingfamily else groupinggeo

            destination_path = os.path.join(base_folder, "Representatives", group_folder, foldername)

        # Create necessary folders, move files, and delete source folder
        create_folder(destination_path)
        add_to_google_sheets(
            occid,
            specimenid,
            phylum,
            taxaclass,
            family,
            subfamily,
            sciname,
            specimen_info.get("identificationQualifier", "") if specimen_info else "",
            catalognumber,
            specimennumber,
            imagetype,
            specimentype,
            specimen_info.get("typestatus", "") if specimen_info else "",
            specimen_info.get("fieldnumber", "") if specimen_info else "",
            boxnumber,
            specimen_info.get("country", "") if specimen_info else "",
            specimen_info.get("stateProvince", "") if specimen_info else "",
            specimen_info.get("island", "") if specimen_info else "",
            specimen_info.get("locality", "") if specimen_info else "",
            plated,
            destination_path
        )
        add_to_digital_ocean()
        if imagetype == "Specimen" and outreachduplicate == "Yes":
            if subfamily:
                outreach_destination_path = os.path.join(base_folder, "Outreach", "Gastropoda", family, subfamily, new_foldername)
                taxaclass = "Gastropoda"
            else:
                outreach_destination_path = os.path.join(base_folder, "Outreach", "Gastropoda", family, new_foldername)
                taxaclass = "Gastropoda"
            create_folder(outreach_destination_path)
            copy_files(os.path.join(staging_folder, foldername, "*"), outreach_destination_path)
        move_files(os.path.join(staging_folder, foldername, "*"), destination_path)
        
        if delete_folder(os.path.join(staging_folder, foldername)):
           rownum = delete_from_staging_sheet(rownum)
           moved_folders_count += 1
        else:
            logging.warning(f"Skipping deletion from staging sheet since folder deletion failed for {foldername}.")
            error_folders_count += 1

        logging.info(f"Successfully moved {foldername} to {destination_path}")

    except Exception as e:
        logging.error(f"Error processing row {row}")
        logging.error(e)
        error_folders_count += 1

def get_remaining_staging_sheet_rows():
    """Fetch the number of rows still in the Google Sheets staging sheet."""
    try:
        range_name = "Form Responses 1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get("values", [])

        remaining_rows = max(len(values) - 1, 0)
        return remaining_rows
    except Exception as e:
        logging.error("Failed to count remaining rows in the staging sheet.")
        logging.error(e)
        return -1
    
def get_remaining_staging_folders():
    """Count the number of folders still in the staging folder on disk."""
    try:
        return len([name for name in os.listdir(staging_folder) if os.path.isdir(os.path.join(staging_folder, name))])
    except Exception as e:
        logging.error("Failed to count remaining folders in the staging folder.")
        logging.error(e)
        return -1

def process_staging_data():
    """Fetches data from Google Sheets and processes each row."""
    global rownum
    logging.info("Fetching staging data from Google Sheets...")

    try:
        range_name = "Form Responses 1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get("values", [])
        processed_values = [row + [""] * (21 - len(row)) for row in values]

        if not values:
            logging.info("No data found in Google Sheets.")
            return

        logging.info(f"Retrieved {len(values) - 1} records from Google Sheets.")
        for row in processed_values[1:]:
            print("\n" + "-" * 50 + "\n")
            process_image_row(row)
            rownum += 1

    except Exception as e:
        logging.critical("Failed to retrieve Google Sheets data!")
        logging.critical(e)
        
    remaining_sheets = get_remaining_staging_sheet_rows()
    remaining_folders = get_remaining_staging_folders()
    
    print("\n" + "=" * 60 + "\n")
    logging.info("SUMMARY")
    logging.info(f"Moved Folders: {moved_folders_count}")
    logging.info(f"Error Folders: {error_folders_count}")
    logging.info(f"Remaining in Staging Sheet: {remaining_sheets if remaining_sheets >= 0 else 'Error retrieving count'}")
    logging.info(f"Remaining in Staging Folder: {remaining_folders if remaining_folders >= 0 else 'Error retrieving count'}")
    print("\n" + "=" * 60 + "\n")

logging.info("Starting the script...")
process_staging_data()
logging.info("Script finished!")

cursor.close()
db_conn.close()
input("Press enter to proceed...")
