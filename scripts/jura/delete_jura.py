# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Duplicate script for Jura data in 2019
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

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker, Metadata

# submodules
from isogeo_migrations_toolbelt import BackupManager


# load .env file
load_dotenv("jura.env", override=True)

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
        Path("./scripts/jura/delete_jura.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    src_cat = environ.get("ISOGEO_CATALOG_SOURCE")
    trg_cat = environ.get("ISOGEO_CATALOG_TARGET")

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

    src_md = isogeo.search(
        group=environ.get("ISOGEO_ORIGIN_WORKGROUP"),
        whole_results=True,
        query="catalog:{}".format(src_cat),
        include="all"
    )

    # listing
    li_md_to_delete = []
    for md in src_md.results:
        metadata = Metadata.clean_attributes(md)
        md_cat = [metadata.tags.get(tag) for tag in metadata.tags if tag.startswith("catalog:")]
        if trg_cat not in md_cat:
            li_md_to_delete.append(metadata._id)
        else:
            pass

    # ################# BACKUP MDs THAT ARE GONNA BE DELETED #######################
    # instanciate backup manager
    backup_path = Path(r"./scripts/jura/_output/_backup")
    backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)

    # lauching backup
    amplitude = 50
    bound_range = int(len(li_md_to_delete) / amplitude)
    li_bound = []
    for i in range(bound_range + 1):
        li_bound.append(amplitude * i)
    li_bound.append(len(li_md_to_delete))

    logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
    for i in range(len(li_bound) - 1):
        bound_inf = li_bound[i]
        bound_sup = li_bound[i + 1]
        logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

        search_parameters = {"query": None, "specific_md": tuple(li_md_to_delete[bound_inf:bound_sup])}
        try:
            backup_mng.metadata(search_params=search_parameters)
        except Exception as e:
            logger.info("an error occured : {}".format(e))

    # ################# DELETE LISTED SRC MDs #######################

    for md in li_md_to_delete:
        isogeo.metadata.delete(md)

    isogeo.close()

