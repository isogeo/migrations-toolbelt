"""
    Name:       Backup script for Dijon data in 2020
    Author:    Isogeo
    Purpose:      Script using the migrations-toolbelt package to backup metadatas.
                Logs are willingly verbose.
    Python:    3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import logging
import csv
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import BackupManager

# load .env file
load_dotenv("dijon.env", override=True)

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
        Path("./scripts/dijon/migration/_logs/backup_dijon.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("######################### BACKUP SESSION #########################")
    logger.info("MAKING METADATA'S ID LIST TO DUPLICATE")

    # ############# LISTING MD TO BACKUP #############

    logger.info("making the list of metadatas source and target uuid")
    li_to_backup = []

    input_csv = Path(r"./scripts/dijon/migration/csv/correspondances.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            if row.get("source_uuid") != "source_uuid":
                li_to_backup.append(row.get("source_uuid"))
                li_to_backup.append(row.get("target_uuid"))
            else:
                pass
    # ############ BACKUP #############
    logger.info(
        "BACKUP {} METADATAS ".format(
            len(li_to_backup)
        )
    )
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

    # backup manager instanciation
    backup_path = Path(r"./scripts/dijon/migration/_output/_backup2")
    backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
    # lauching backup
    amplitude = 50
    bound_range = int(len(li_to_backup) / amplitude)
    li_bound = []
    for i in range(bound_range + 1):
        li_bound.append(amplitude * i)
    li_bound.append(len(li_to_backup))

    logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
    for i in range(len(li_bound) - 1):
        bound_inf = li_bound[i]
        bound_sup = li_bound[i + 1]
        logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

        search_parameters = {"query": None, "specific_md": tuple(li_to_backup[bound_inf:bound_sup])}
        try:
            backup_mng.metadata(search_params=search_parameters)
        except Exception as e:
            logger.info("an error occured : {}".format(e))

    isogeo.close()
