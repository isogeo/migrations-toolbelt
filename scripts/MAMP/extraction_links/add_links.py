# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to add extraction link to "Métropole Aix Marseille Provence" metadatas
    Author:       Isogeo
    Purpose:      Script using isogeo-pysdk to update events.

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
from isogeo_pysdk import Isogeo, Link
# load .env file
load_dotenv("./env/mamp.env", override=True)

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
        Path("./scripts/misc/events/_logs/add_links.log"),
        "a",
        5000000,
        1,
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # shortcut
    url_base = "https://geodata.ampmetropole.fr/?mode_id=extraction&layer="

    # Retrieving infos involved metadatas from csv report file
    input_csv = Path(r"./scripts/MAMP/extraction_links/li_metadatas_names.csv")
    fieldnames = [
        "md_name"
    ]
    li_md_names = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)
        for row in reader:
            if reader.line_num > 1:
                li_md_names.append(row.get("md_name"))
            else:
                pass
    nb_to_parse = len(li_md_names)

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

    whole_search = isogeo.search(
        whole_results=True,
        group=environ.get("ISOGEO_ORIGIN_WORKGROUP")
    )

    # filter involved metadatas
    li_md = [md for md in whole_search.results if md.get("name")]
    li_md_to_parse = [md for md in li_md if md.get("name") in li_md_names]

    nb_to_parse = len(li_md_to_parse)

    # First, let inspected workgroups looking for metadatas with events that need to be cleaned.
    for md in li_md_to_parse:
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

        # retrieve metadata object
        metadata = isogeo.metadata.get(md.get("_id"))

        # build extraction URL from data name
        extraction_url = url_base + md.get("name")

        # build link object
        extraction_link = Link()
        extraction_link.url = extraction_url
        extraction_link.type = "url"
        extraction_link.kind = "data"
        extraction_link.action = "download"

        # add the link to the metadata
        isogeo.metadata.links.create(metadata, extraction_link)

    isogeo.close()

    csv_path = Path(r"./scripts/misc/events/csv/corrupted_v12.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "wg_name",
                "wg_uuid",
                "md_uuid",
                "event_uuid",
                "event_date",
                "event_description",
                "issue",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
