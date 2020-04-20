# -*- coding: UTF-8 -*-
#! python3

"""
    Name:       Duplicate script for Dijon data in 2020
    Author:    Isogeo
    Purpose:      Script using the migrations-toolbelt package to backup metadatas.
                Logs are willingly verbose.
    Python:    3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import BackupManager, SearchReplaceManager

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
        Path("./scripts/dijon/search_and_replace/backup_dijon.log"), "a", 5000000, 1
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

    # ############# LISTING MD TO DUPLICATE #############

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

    replace_patterns = {
        "title": ("Grand Dijon", "Dijon Métropole"),
        "abstract": ("Grand Dijon", "Dijon Métropole"),
    }

    dict_prepositions = {
        "la Communauté Urbaine du ": "",
        "au ": "à ",
        "du ": "de ",
        "le ": "",
    }

    sr_mng = SearchReplaceManager(
        api_client=isogeo,
        attributes_patterns=replace_patterns,
        prepositions=dict_prepositions,
    )

    search_params = {
        "group": environ.get("ISOGEO_ORIGIN_WORKGROUP"),
        "whole_results": True,
    }
    # logger.info("search metadatas into client work group")
    wg_md_search = isogeo.search(**search_params)

    # Keeping only metadata matching with replace pattern
    logger.info("making the list of metadatas id matching the pattern")
    tup_to_backup = sr_mng.filter_matching_metadatas(wg_md_search.results)
    li_to_backup = [md._id for md in tup_to_backup]

    # ############ BACKUP #############
    logger.info(
        "BACKUP {} METADATAS ".format(
            len(li_to_backup)
        )
    )
    # instanciate backup manager
    backup_path = Path(r"./scripts/dijon/search_and_replace/_output/_backup")
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
