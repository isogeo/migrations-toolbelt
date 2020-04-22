# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         One of the scripts to perform cadastre GEOMAP migration service for AMP
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata duplication.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load .env file
load_dotenv("./env/amp.env", override=True)

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
        Path("./scripts/AMP/md_ref/_logs/duplicate_trgcat_into_srcwg.log"), "a", 5000000, 1
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
    # GET CATALOG TO DUPLICATE
    amp_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    origin_cat_uuid = environ.get("AMP_CADATRE_CATALOG_UUID")
    dgfip_wg_uuid = environ.get("ISOGEO_DGFIP_WORKGROUP")
    destination_cat_uuid = environ.get("SOURCE_CADASTRE_CATALOG_UUID")

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
    logger.info("Isogeo client instanciated after {}.s".format(auth_timer))

    # make the search on target catalog
    logger.info("Retrieving metadatas to import from {} wg to {} wg ".format(amp_wg_uuid, dgfip_wg_uuid))
    origin_search = isogeo.search(
        group=amp_wg_uuid,
        query="catalog:{}".format(origin_cat_uuid),
        whole_results=True
    )
    logger.info("{} metadata found into {} catalog".format(origin_search.total, origin_cat_uuid))

    # retrieve destination catalog
    logger.info("Retrieving destination catalog {}".format(destination_cat_uuid))
    destination_cat = isogeo.catalog.get(
        workgroup_id=dgfip_wg_uuid,
        catalog_id=destination_cat_uuid,
    )

    # list used to create futur maping table for cadastre_geomap migration
    li_matching_table = []

    for md in origin_search.results:

        if default_timer() - auth_timer >= 250:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        duplicator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=md.get("_id")
        )

        duplicated_md = duplicator.duplicate_into_other_group(
            destination_workgroup_uuid=dgfip_wg_uuid,
            copymark_abstract=False,
            copymark_title=False,
        )

        isogeo.catalog.associate_metadata(metadata=duplicated_md, catalog=destination_cat)

        li_matching_table.append(
            (
                md.get("name"),
                md.get("_id"),
                duplicated_md._id
            )
        )

    csv_path = Path(r"./scripts/AMP/md_ref/cadastre_geomap/csv/matching.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "target_name",
                "target_uuid",
                "source_uuid"
            ]
        )
        for data in li_matching_table:
            writer.writerow(data)
