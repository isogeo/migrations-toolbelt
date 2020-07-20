# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to remove fake events added by Isogeo Scan
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
import datetime
import re
from pprint import pprint

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/events.env", override=True)

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
        Path("./scripts/misc/events/_logs/events_cleaner.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("{} Isogeo workgroups will be inspected".format(len(li_wg_uuid)))

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

    date_ref = datetime.datetime(2020, 5, 8)

    # Retrieving Isogeo involved workgroups uuid and infos
    li_wg_uuid = environ.get("ISOGEO_INVOLVED_WORKGROUPS").split(";")
    li_wg = [isogeo.workgroup.get(wg_uuid) for wg_uuid in li_wg_uuid]

    li_for_csv = []
    # First, let inspected workgroups looking for metadatas with events that need to be cleaned.
    for wg in li_wg:
        logger.info("Inspecting '{}' workgroup ({})".format(wg._id, wg.name))
        # Retrieve all workgroup's metadatas
        wg_search = isogeo.search(
            group=wg._id,
            whole_results=True
        )
        logger.info("{} metadatas retireved from '{}' workgroup".format(wg_search.total, wg.name))

        wg_md = wg_search.results
        for md in wg_md:
            # Only parse metadata with event and filter them on last update date
            str_modified = md.get("_modified").split("T")[0]
            date_modified = datetime.datetime.strptime(str_modified, "%Y-%m-%d")
            if md._modified > date_ref and len(md.get("events")):
                md_events = [event for event in md.get("events") if event.get("kind") == "update"]

                for event in md_events:
                    description = event.get("description")
                    line_for_csv = [event.get("_id"), description, md.get("_id"), wg._id]

                    if description.startswith("undefined") or description.startswith("eventDescription"):
                        line_for_csv.append("to_delete")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif "undefined" in description:
                        line_for_csv.append("to_clean")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif description.startswith("The dataset has been modified :"):
                        li_event_items = description.split("* ")[1:]
                        for item in li_event_items:
                            if re.search(r'\sfrom\s[a-zA-Z0-9_]*\sto\s[a-zA-Z0-9_]*', item):
                                line_for_csv.append("to_clean")
                                li_for_csv.append(line_for_csv)
                            break
                        continue

