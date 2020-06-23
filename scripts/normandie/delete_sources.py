# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to delete Normandie source metadatas using the MetadataDeleter
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
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import BackupManager, MetadataDeleter


# load .env file
load_dotenv("./env/normandie.env", override=True)

checker = IsogeoChecker()

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
        Path("./scripts/normandie/_logs/delter_source_normandie.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # ################# MAKE THE LIST OF SRC MD'S UUID TO DELETE #######################
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

    # Retrieve metadata to delet UUIDs from csv file
    li_dlt_uuid = []

    # prepare csv reading
    input_csv = Path(r"./scripts/normandie/csv/migrated.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        row_num = 0
        for row in reader:
            row_num += 1
            src_uuid = row.get("source_uuid")
            src_title = row.get("source_title")
            src_name = row.get("source_name")
            trg_name = row.get("target_name")
            trg_uuid = row.get("target_uuid")
            if src_uuid != "source_uuid":
                li_dlt_uuid.append(src_uuid)

    logger.info("------- {} source metadatas listed gonna be backuped then deleted -------".format(len(li_dlt_uuid)))
    # ################# BACKUP MDs THAT ARE GONNA BE DELETED #######################
    # instanciate backup manager
    if environ.get("BACKUP") == "1" and len(li_dlt_uuid) > 0:
        logger.info("---------------------------- BACKUP ---------------------------------")
        # backup manager instanciation
        backup_path = Path(r"./scripts/normandie/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_dlt_uuid) > amplitude:
            bound_range = int(len(li_dlt_uuid) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_dlt_uuid))

            logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
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

                search_parameters = {"query": None, "specific_md": tuple(li_dlt_uuid[bound_inf:bound_sup])}
                try:
                    backup_mng.metadata(search_params=search_parameters)
                except Exception as e:
                    logger.info("an error occured : {}".format(e))
        else:
            search_parameters = {"query": None, "specific_md": tuple(li_dlt_uuid)}
            backup_mng.metadata(search_params=search_parameters)
    else:
        pass

    # ################# DELETE LISTED SRC MDs #######################
    logger.info("------- Starting to delete source metadatas -------")

    md_dltr = MetadataDeleter(api_client=isogeo)
    md_dltr.metadata(metadata_ids_list=li_dlt_uuid)

    isogeo.close()
