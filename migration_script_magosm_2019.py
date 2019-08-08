# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for magOSM data in 2019
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import asyncio
import csv
import logging
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
import urllib3

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import IsogeoChecker, IsogeoSession

# submodules
from migrations_toolbelt import BackupManager, MetadataDuplicator

# #############################################################################
# ######## Globals #################
# ##################################

# environment vars
load_dotenv(".env", override=True)
# load_dotenv("prod.env", override=True)

# ignore warnings related to the QA self-signed cert
if environ.get("ISOGEO_PLATFORM").lower() == "qa":
    urllib3.disable_warnings()

checker = IsogeoChecker()

# ##############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.DEBUG)
    # logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler("migration_magosm.log", "a", 5000000, 1)
    log_file_handler.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # chronometer
    START_TIME = default_timer()

    # prepare lists and dicts
    li_to_backup = []
    li_to_migrate = []  # it'll be populate as a list of tuples

    # read csv file
    # structure CSV : title;old_name;old_uri;new_name;new_uri
    input_csv = Path(r"./input/sample_migration_import.csv")

    # parse input CSV and perform operations
    logger.info("Start reading the input file")
    with input_csv.open() as csvfile:
        # read it
        fieldnames = ["title", "old_name", "old_uri", "new_name", "new_uri"]
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        # parse csv
        for row in reader:
            # check uuids
            if not checker.check_is_uuid(row.get("old_uri")):
                logger.error(
                    "UUID '{}' is not correct. It'll be ignored.".format(
                        row.get("old_uri")
                    )
                )
                continue
            if not checker.check_is_uuid(row.get("old_uri")):
                logger.error(
                    "UUID '{}' is not correct. It'll be ignored.".format(
                        row.get("old_uri")
                    )
                )
                continue

            # prepare backup
            li_to_backup.extend([row.get("old_uri"), row.get("new_uri")])

            # prepare migration
            li_to_migrate.append((row.get("old_uri"), row.get("new_uri")))

    csv_timer = default_timer() - START_TIME
    logger.info(
        "Read finished in {:5.2f}s. {} metadata will be saved (backup) and {} will me migrated.".format(
            csv_timer, len(li_to_backup), len(li_to_migrate)
        )
    )

    # API connection
    # establish isogeo connection
    isogeo = IsogeoSession(
        client_id=environ.get("ISOGEO_API_USER_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_CLIENT_SECRET"),
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
    )

    # getting a token
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )

    auth_timer = default_timer() - csv_timer
    logger.info(
        "Connection to Isogeo established in {:5.2f}s.".format(
            auth_timer
        )
    )

    # BACKUP
    backup_mngr = BackupManager(api_client=isogeo, output_folder="./output")
    search_parameters = {"query": None, "specific_md": li_to_backup}
    backup_mngr.metadata(search_params=search_parameters)

    bkp_timer = default_timer() - auth_timer
    logger.info(
        "Backup of {} finished in {:5.2f}s.".format(
            len(li_to_backup), bkp_timer
        )
    )

    # MIGRATION
    prev_timer = bkp_timer
    for tup_to_migrate in li_to_migrate:
        # prepare source for migration
        migrator = MetadataDuplicator(
            api_client=isogeo,
            source_metadata_uuid=tup_to_migrate[0]
        )

        # import
        new_md = migrator.import_into_other_metadata(
            destination_metadata_uuid=tup_to_migrate[1],
            copymark_catalog="3dda5db6cbea4c48b0357d47d95ebf19",
            switch_service_layers=1,
        )

        # log it
        logger.info(
            "Migration from {} to {} finished in {:5.2f}s.".format(
                tup_to_migrate[0], tup_to_migrate[1], default_timer() - prev_timer
            )
        )
        prev_timer = default_timer()

    mig_total_timer = default_timer() - bkp_timer
    logger.info(
        "Migration of {} finished in {:5.2f}s.".format(
            len(li_to_migrate), mig_total_timer
        )
    )

    # display elapsed time
    time_completed_at = "{:5.2f}s".format(default_timer() - START_TIME)
    print("Process finished in {}".format(time_completed_at))

    # close connection
    isogeo.close()
