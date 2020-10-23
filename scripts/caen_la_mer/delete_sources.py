# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to delete "Caen la mer" source metadata after the migration was performed
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
import csv

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import BackupManager, MetadataDeleter


# load .env file
load_dotenv("./env/caen.env", override=True)

if __name__ == "__main__":
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/caen_la_mer/_logs/delete_caen.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # ################# RETRIEVE THE LIST OF SRC MD'S UUID TO DELETE FROM CSV FILE #######################
    li_md_to_delete = []
    # prepare csv reading
    input_csv = Path(r"./scripts/caen_la_mer/csv/migrated_1603210003.014903.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_uuid = row.get("source_uuid")
            if src_uuid != "source_uuid":
                # check if the target metadata exists
                li_md_to_delete.append(src_uuid)
            else:
                pass

    # ################## BACKUP THEN DELETE METADATAS RETRIEVED ##################
    # API client instanciation
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
        auth_mode="user_legacy",
    )
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )
    auth_timer = default_timer()

    logger.info("------- {} source metadatas listed gonna be backuped then deleted -------".format(len(li_md_to_delete)))
    # ################# BACKUP MDs THAT ARE GONNA BE DELETED #######################

    # ------------------------------------ BACKUP --------------------------------------
    if environ.get("BACKUP") == "1" and len(li_md_to_delete):
        logger.info("---------------------------- BACKUP ---------------------------------")
        # instanciate backup manager
        backup_path = Path(r"./scripts/caen_la_mer/_output/_backup_deleted")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50
        bound_range = int(len(li_md_to_delete) / amplitude)
        li_bound = []
        for i in range(bound_range + 1):
            li_bound.append(amplitude * i)
        li_bound.append(len(li_md_to_delete))

        logger.info("------- Starting backup for {} rounds -------".format(len(li_bound) - 1))
        for i in range(len(li_bound) - 1):
            if default_timer() - auth_timer >= 250:
                logger.info("Manually refreshing token")
                backup_mng.isogeo.connect(
                    username=environ.get("ISOGEO_USER_NAME"),
                    password=environ.get("ISOGEO_USER_PASSWORD"),
                )
                auth_timer = default_timer()
            else:
                pass
            bound_inf = li_bound[i]
            bound_sup = li_bound[i + 1]
            logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

            search_parameters = {"query": None, "specific_md": tuple(li_md_to_delete[bound_inf:bound_sup])}
            try:
                backup_mng.metadata(search_params=search_parameters)
            except Exception as e:
                logger.info("an error occured : {}".format(e))

    # ################# DELETE LISTED SRC MDs #######################
    logger.info("------- Starting to delete {} source metadatas -------".format(len(li_md_to_delete)))
    md_dltr = MetadataDeleter(api_client=isogeo)
    md_dltr.delete(li_md_to_delete)

    isogeo.close()
