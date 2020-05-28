# -*- coding: UTF-8 -*-
#! python3

"""
    Name:       Duplicate script for Dijon data in 2020
    Author:    Isogeo
    Purpose:      Script using the migrations-toolbelt package to duplicate metadatas into 'Isogeo Migrations'
                workgroup for purpose of testing migration scripts.
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
from isogeo_migrations_toolbelt import MetadataDuplicator, SearchReplaceManager

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
        Path("./scripts/dijon/search_and_replace/duplicate_dijon.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n------------ DUPLICATING SESSION ------------")
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

    sr_mngr = SearchReplaceManager(
        api_client=isogeo,
        attributes_patterns=replace_patterns,
        prepositions=dict_prepositions,
    )

    search_params = {
        "group": environ.get("ISOGEO_ORIGIN_WORKGROUP"),
        "whole_results": True,
    }
    logger.info("search metadatas into client work group")
    wg_md_search = isogeo.search(**search_params)

    # Keeping only metadata matching with replace pattern
    logger.info("filter matching metadata")
    tup_to_duplicate = sr_mngr.filter_matching_metadatas(wg_md_search.results)
    li_to_duplicate = [md._id for md in tup_to_duplicate]
    print(len(li_to_duplicate))

    logger.info(
        "DUPLICATING {} METADATAS INTO 'ISOGEO MIGRATIONS' WORKGROUP".format(
            int(len(li_to_duplicate) / 10)
        )
    )
    # ############# DUPLICATING #############

    # to build mapping table of 'Isogeo Migrations' workgroup
    li_migrated = []
    li_failed = []
    index = 0
    for md_id in li_to_duplicate:
        # to suplicate a sample
        if index % 10 == 0:
            logger.info(
                "-------------- Duplicating {}/{} metadata ({}) -------------- ".format(
                    len(li_migrated) + 1, int(len(li_to_duplicate) / 10 + 1), md_id
                )
            )
            # loading the metadata to duplicate from his UUID
            try:
                md_migrator = MetadataDuplicator(
                    api_client=isogeo, source_metadata_uuid=md_id
                )
            except Exception as e:
                logger.info("Faile to load md {} : \n {}".format(md_id, e))
                li_failed.append(md_id)
                index += 1
                continue

            md_loaded = md_migrator.metadata_source

            # check if the metadata exists
            if isinstance(md_loaded, tuple):
                logger.info(
                    "{} - There is no accessible source metadata corresponding to this "
                    "uuid".format(md_id)
                )
                pass
            # checks metadata name and title indicated in the mapping table
            # then, dupplicate the metadata
            else:
                try:
                    md_migrated = md_migrator.duplicate_into_other_group(
                        destination_workgroup_uuid=environ.get(
                            "ISOGEO_MIGRATION_WORKGROUP"
                        ),
                        copymark_abstract=True,
                        copymark_title=True,
                    )
                    li_migrated.append(md_migrated._id)
                except Exception as e:
                    logger.info(
                        "Faile to import md '{}' into 'Isogeo Migrations' work group : \n {}".format(
                            md_id, e
                        )
                    )
                    li_failed.append(md_id)
                    index += 1
                    continue

                index += 1
        else:
            index += 1
            continue

    isogeo.close()

    csv_sample = Path(r"./scripts/dijon/search_and_replace/_output/duplicated.csv")
    with open(csv_sample, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(["md_id"])
        for data in li_migrated:
            writer.writerow([data])

    if len(li_failed) > 0:
        logger.info("{} metadatas haven't been duplicated".format(len(li_failed)))
        csv_failed = Path(r"./scripts/dijon/search_and_replace/_output/duplicate_failed.csv")
        with open(csv_failed, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(["md_id"])
            for data in li_failed:
                writer.writerow([data])
    else:
        logger.info("All metadatas have been duplicated :)")
