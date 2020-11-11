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
from datetime import datetime

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, Metadata, Event

# load .env file
load_dotenv("./env/events.env", override=True)

# #############################################################################
# ############ Functions ################
# #######################################


# Print iterations progress
def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


# #############################################################################
# ########## Main program ###############
# #######################################

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
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

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
    nb_to_parse = len(li_events_to_clean)

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

    nb_parsed = 0
    nb_updated = 0

    current_md_uuid = ""
    li_for_csv = []
    for tup in li_events_to_clean:
        printProgressBar(
            iteration=nb_parsed,
            total=nb_to_parse,
            prefix='Processing progress:',
            length=100
        )
        nb_parsed += 1
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
        # check API response
        if isinstance(md, Metadata):
            pass
        else:
            logger.warning("Cannot retrieve '{}' metadata object : {}".format(current_md_uuid, md))
            continue

        # retrieve event object
        event = isogeo.metadata.events.event(metadata_id=tup[2], event_id=tup[3])
        # check API response
        if isinstance(event, Event):
            pass
        else:
            logger.warning("Cannot retrieve '{}' event object : {}".format(tup[3], event))
            continue

        if " attribute attribute " in event.description:
            new_description = event.description.replace(" attribute attribute ", " attribute ")

            li_for_csv.append([
                tup[0],
                tup[1],
                md._id,
                event._id,
                event.description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                new_description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                "updated"
            ])

            event.description = new_description
            if int(environ.get("HARD_MODE")):
                isogeo.metadata.events.update(event=event, metadata=md)
            else:
                pass
            nb_updated += 1
        else:
            continue

    isogeo.close()

    logger.info("--> {} corrupted events have been processed including :".format(nb_parsed))
    logger.info("  - {} updated".format(nb_updated))
    logger.info("  - {} skipped\n".format(nb_to_parse - nb_updated))

    csv_path = Path("./scripts/misc/events/csv/attributeattribute_cleaner_{}.csv".format(str(datetime.now().timestamp()).split(".")[0]))
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "wg_name",
                "wg_uuid",
                "md_uuid",
                "md_uuid",
                "event_uuid",
                "event_description",
                "event_description_cleaned",
                "to_do"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
