# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to clean "attribute attribute" corrupted events added by Isogeo Scan
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
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker
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
        Path("./scripts/misc/events/_logs/attributeattribute_events_cleaner.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    # Retrieving infos about corrupted events from csv report file
    input_csv = Path(r"./scripts/misc/events/csv/corrupted.csv")
    fieldnames = [
        "wg_name",
        "wg_uuid",
        "md_uuid",
        "event_uuid",
        "event_date",
        "event_description",
        "issue"
    ]
    li_events_to_clean = []
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter="|", fieldnames=fieldnames)

        for row in reader:
            wg_name = row.get("wg_name")
            wg_uuid = row.get("wg_uuid")
            md_uuid = row.get("md_uuid")
            event_uuid = row.get("event_uuid")
            issue = row.get("issue")
            if issue == "undefined":
                li_events_to_clean.append(
                    (
                        wg_name,
                        wg_uuid,
                        md_uuid,
                        event_uuid
                    )
                )
            else:
                pass

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

    current_md_uuid = ""
    li_for_csv = []
    for tup in li_events_to_clean:
        # refresh token if needed
        if default_timer() - auth_timer >= 230:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        # retrieve metadata object
        if current_md_uuid != tup[2]:
            current_md_uuid = tup[2]
            md = isogeo.metadata.get(current_md_uuid)
        else:
            pass

        event = isogeo.metadata.events.event(metadata_id=tup[2], event_id=tup[3])

        if " attribute attribute " in event.description:
            new_description = event.description.replace(" attribute attribute ", " attribute ")

            li_for_csv.append([
                md._id,
                event._id,
                event.description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                new_description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                "to_delete"
            ])

            event.description = new_description
            # isogeo.metadata.events.update(event=event, metadata=md)
        else:
            continue

    isogeo.close()

    csv_path = Path(r"./scripts/misc/events/csv/attributeattribute_cleaner.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "md_uuid",
                "event_uuid",
                "event_description",
                "event_description_cleaned",
                "to_do"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
