# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to update metadata title according to a matching table
    Author:       Isogeo
    Purpose:      Script using isogeo-pysdk to update title.

    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
from os import environ
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from timeit import default_timer

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    Metadata
)

# load .env file
load_dotenv("./env/misc.env", override=True)

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
        Path("./scripts/misc/title/_logs/bdcarto_title_update.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # Retrieving infos from CSV matching table
    csv_file_path = Path(r"./scripts/misc/title/csv/correspondances.csv")
    fieldnames = [
        "current_title",
        "new_title",
        "md_uuid",
    ]
    li_infos = []
    li_md_uuid = []
    with csv_file_path.open(mode="r", encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)
        for row in reader:
            if reader.line_num > 1:
                tup_info = (
                    row.get("md_uuid"),
                    row.get("current_title"),
                    row.get("new_title"),
                )
                li_infos.append(tup_info)
                li_md_uuid.append(row.get("md_uuid"))
            else:
                pass
    logger.info("Infos retrieved about {} metadatas from '{}' matching table".format(len(li_infos), csv_file_path))

    # Isogeo ressources UUID
    ignf_wg_uuid = environ.get("ISOGEO_INGF_WORKGROUP")
    bdcarto32_cat_uuid = environ.get("ISOGEO_BDCARTO_CAT")

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

    # Ask Isogeo API about metadatas from relevant catalog
    bd_carto_search = isogeo.search(
        group=ignf_wg_uuid,
        query="catalog:{}".format(bdcarto32_cat_uuid),
        whole_results=True
    )
    logger.info("{} metadatas retrieved from {} catalog".format(bd_carto_search.total, bdcarto32_cat_uuid))

    # Browsing metadatas retrieved from Isogeo API and updating their title
    md_parsed_count = 0
    for md in bd_carto_search.results:

        # refresh token if needed
        if default_timer() - auth_timer >= 6900:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        # Check if the metadata appears in matching table
        if md.get("_id") in li_md_uuid:
            md_info = [info for info in li_infos if info[0] == md.get("_id")][0]
            # check if infos retrieved from matching table are consistent
            if md_info[1] == md.get("title"):
                isogeo_md = Metadata(**md)
                isogeo_md.title = md_info[2]
                # let's updtae metadata title
                isogeo.metadata.update(metadata=isogeo_md)
                md_parsed_count += 1
            else:
                logger.warning("Infos retrieved from matching table about '{}' metadata are not consistent with API response".format(md.get("_id")))
        else:
            pass

    logger.info("{}/{} metadatas parsed".format(md_parsed_count, len(li_infos)))
