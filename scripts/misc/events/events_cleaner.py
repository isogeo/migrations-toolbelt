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
import json

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
    Event,
    Metadata
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

    # md = isogeo.metadata.get("eafade7e119f45eb82c068f39c1cc0fa", include=("events", ))
    # with open("scripts/misc/events/_output/eafade7e119f45eb82c068f39c1cc0fa.json", "w", encoding="UTF-8") as outfile:
    #     json.dump(md.to_dict(), outfile, sort_keys=True, indent=4)

    date_ref = datetime.datetime(2020, 5, 6)

    # Retrieving Isogeo involved workgroups uuid and infos
    li_wg_uuid = environ.get("ISOGEO_INVOLVED_WORKGROUPS").split(";")
    li_wg = [isogeo.workgroup.get(wg_uuid) for wg_uuid in li_wg_uuid]
    logger.info("{} Isogeo workgroups will be inspected".format(len(li_wg_uuid)))

    li_for_csv = []
    li_event_to_parse = []
    # First, let inspected workgroups looking for metadatas with events that need to be cleaned.
    for wg in li_wg:
        nb_per_round = 0
        logger.info("\n")
        logger.info("Inspecting '{}' workgroup ({})".format(wg.name, wg._id))
        # Retrieve all workgroup's metadatas
        wg_search = isogeo.search(
            group=wg._id,
            whole_results=True,
            include=("events", )
        )
        logger.info("{} metadatas retireved from '{}' workgroup".format(wg_search.total, wg.name))

        wg_md = wg_search.results
        for md in wg_md:
            # Only parse metadata with event and filter them on last update date
            str_modified = md.get("_modified").split("T")[0]
            date_modified = datetime.datetime.strptime(str_modified, "%Y-%m-%d")
            if date_modified > date_ref and len(md.get("events")):
                md_events = [event for event in md.get("events") if event.get("description") and event.get("kind") == "update"]

                for event in md_events:
                    description = event.get("description").replace("\n", "\\n").replace("\r", "\\r")
                    line_for_csv = [event.get("_id"), description, md.get("_id"), wg._id, wg.name]

                    if description.startswith("undefined") or description.startswith("eventDescription"):
                        line_for_csv.append("to_delete")
                        li_for_csv.append(line_for_csv)
                        nb_per_round += 1
                        li_event_to_parse.append(("to_delete", event, md))
                        continue

                    elif "undefined" in description:
                        line_for_csv.append("to_clean")
                        li_for_csv.append(line_for_csv)
                        nb_per_round += 1
                        li_event_to_parse.append(("to_clean", event, md))
                        continue

                    elif description.startswith("The dataset has been modified :"):
                        # print("{}".format(description))
                        li_event_items = description.split("\\n* ")[1:]
                        for item in li_event_items:
                            if re.search(r'\sfrom\s[a-zA-Z0-9_]*\sto\s[a-zA-Z0-9_]*', item):
                                line_for_csv.append("to_clean")
                                li_for_csv.append(line_for_csv)
                                nb_per_round += 1
                                li_event_to_parse.append(("to_clean", event, md))
                            else:
                                pass
                            break
                        continue

                    else:
                        pass
            else:
                pass
        logger.info("{} corrupted events retrieved into '{}' worgroup's metadatas".format(nb_per_round, wg.name))

    nb_event_deleted = 0
    nb_event_cleaned = 0
    for tup in li_event_to_parse:
        event = Event(**tup[1])
        md = Metadata.clean_attributes(**tup[2])
        if tup[0] == "to_delete":
            # isogeo.metadata.events.delete(event=event, metadata=md)
            nb_event_deleted += 1
        elif tup[0] == "to_clean":
            description = event.description
            re.sub("undefined", "", description)
            # re.sub(r'\sfrom\s[a-zA-Z0-9_]*\sto\s[a-zA-Z0-9_]*', "", description)

            nb_event_cleaned += 1
        else:
            logger.info("Unexpected event to parse type : {}".format(tup[0]))

    csv_path = Path(r"./scripts/misc/events/csv/corrupted.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "event_uuid",
                "event_description",
                "md_uuid",
                "wg_uuid",
                "wg_name",
                "to_do"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)

