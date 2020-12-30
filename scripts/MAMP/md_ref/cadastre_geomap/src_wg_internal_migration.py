# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         One of the scripts to perform cadastre GEOMAP migration service for AMP
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
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

# load dijon.env file
load_dotenv("./env/mamp.env", override=True)

if __name__ == "__main__":
    # logs
    logger = logging.getLogger()
    # ------------ Log & debug ----------------
    logging.captureWarnings(True)
    logger.setLevel(logging.INFO)

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/AMP/md_ref/_logs/migrate_cadastre_into_src_wg.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n------------ MIGRATION SESSION ------------")
    # ################# RETRIEVE INFORMATIONS NEEDED TO PROCEED MIGRATION #################
    # Retrieve subressources uuid
    origin_cat_uuid = environ.get("DGFIP_CADASTRE_CATALOG_UUID")
    destination_cat_uuid = environ.get("SOURCE_CADASTRE_CATALOG_UUID")
    wg_uuid = environ.get("ISOGEO_DGFIP_WORKGROUP")

    # Retrieve informations about source and target md from matching table and prepare csv reading
    input_csv = Path(r"./scripts/AMP/md_ref/cadastre_geomap/csv/matching_cadastre_intramigration.csv")
    fieldnames = [
        "trg_name",
        "src_title",
    ]
    logger.info("Retrieving informations from matching table {}".format(input_csv))

    li_to_migrate = []

    # csv reading
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)
        for row in reader:
            trg_name = row.get("trg_name")
            src_title = row.get("src_title")
            # if trg_name != "trg_name": PROD
            if trg_name == "aopcad.renvoi":
                matching_infos = (trg_name, src_title)
                li_to_migrate.append(matching_infos)
            else:
                pass
    logger.info("{} migration couples retrieved from matching table".format(len(li_to_migrate)))

    # ############################### MIGRATING ###############################
    # API client instanciation
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
        auth_mode="user_legacy",
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
    )
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )
    auth_timer = default_timer()

    # RETRIEVING ORIGIN AN DDESTINATION MD UUIDs
    # retrieving origin md
    search_origin_md = isogeo.search(
        group=wg_uuid,
        query="catalog:{}".format(origin_cat_uuid),
        whole_results=True
    )
    source_md = search_origin_md.results
    li_src_title = [md.get("title").strip() for md in source_md]
    logger.info("{} source metadatas retrieved".format(search_origin_md.total))

    # retrieving destination md
    search_destination_md = isogeo.search(
        group=wg_uuid,
        query="catalog:{}".format(destination_cat_uuid),
        whole_results=True
    )
    target_md = search_destination_md.results
    li_trg_name = [md.get("name").strip() for md in target_md]
    logger.info("{} target metadatas retrieved".format(search_destination_md.total))

    # a little check to compare number of source and target metadatas
    if search_destination_md.total != search_origin_md.total:
        logger.warning("target md number is different from source md number : {} vs {}".format(search_destination_md.total, search_origin_md.total))
    else:
        pass

    logger.info("Starting migration")
    index = 0
    for md_info in li_to_migrate:
        src_title = md_info[1]
        trg_name = md_info[0]

        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_to_migrate)))

        # check if source md was found into origin catalog
        if src_title.strip() not in li_src_title:
            logger.error("{} metadata not found into {} source catalog".format(src_title, origin_cat_uuid))
            index += 1
            continue
        else:
            pass
        # check if target md was found into destination catalog
        if trg_name not in li_trg_name:
            logger.error("{} metadata not found into {} target catalog".format(trg_name, destination_cat_uuid))
            index += 1
            continue
        else:
            pass

        src_md_uuid = [md.get("_id") for md in source_md if md.get("title").strip() == src_title.strip()][0]
        trg_md_uuid = [md.get("_id") for md in target_md if md.get("name").strip() == trg_name.strip()][0]

        if src_md_uuid and trg_md_uuid:
            # Refresh api clien token if needed
            if default_timer() - auth_timer >= 6900:
                logger.info("Manually refreshing token")
                isogeo.connect(
                    username=environ.get("ISOGEO_USER_NAME"),
                    password=environ.get("ISOGEO_USER_PASSWORD"),
                )
                auth_timer = default_timer()
            else:
                pass

            logger.info("from {} to {} :".format(src_title, trg_name))

            # instanciate MetadataDuplicator
            migrator = MetadataDuplicator(
                api_client=isogeo, source_metadata_uuid=src_md_uuid
            )
            # migrate
            migrated = migrator.import_into_other_metadata(
                destination_metadata_uuid=trg_md_uuid,
                copymark_abstract=False,
                copymark_title=False
            )

        else:
            logger.error("faile to retrieve target or source md from api content reply")
            index += 1
            continue

        index += 1
    isogeo.close()
