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
import json
from timeit import default_timer

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
    Event
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
    auth_timer = default_timer()

    md = isogeo.metadata.get("eafade7e119f45eb82c068f39c1cc0fa", include=("events", ))
    with open("scripts/misc/events/_output/eafade7e119f45eb82c068f39c1cc0fa.json", "w", encoding="UTF-8") as outfile:
        json.dump(md.to_dict(), outfile, sort_keys=True, indent=4)

    # Retrieving Isogeo involved workgroups uuid and infos
    li_wg_uuid = environ.get("ISOGEO_INVOLVED_WORKGROUPS").split(";")  # PROD
    li_wg = [isogeo.workgroup.get(wg_uuid) for wg_uuid in li_wg_uuid]
    logger.info("{} Isogeo workgroups will be inspected".format(len(li_wg_uuid)))

    li_for_csv = []
    li_event_to_parse = []
    # First, let inspected workgroups looking for metadatas with events that need to be cleaned.
    for wg in li_wg:
        nb_per_round = 0
        logger.info("\n")
        logger.info("Inspecting '{}' workgroup ({})".format(wg.name, wg._id))

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

        # Retrieve all workgroup's metadatas
        wg_search = isogeo.search(
            group=wg._id,
            whole_results=True,
            include=("events", )
        )
        logger.info("{} metadatas retrieved from '{}' workgroup".format(wg_search.total, wg.name))

        wg_md = wg_search.results
        for md in wg_md:
            # Only parse metadata with event and filter them on last update date
            str_modified = md.get("_modified").split("T")[0]
            date_modified = datetime.datetime.strptime(str_modified, "%Y-%m-%d")
            if len(md.get("events")):
                md_events = [event for event in md.get("events") if event.get("description") and event.get("kind") == "update"]

                for event in md_events:
                    description = event.get("description").replace("\n", "\\n").replace("\r", "\\r")
                    line_for_csv = [event.get("_id"), event.get("date"), description.replace(";", ""), md.get("_id"), wg._id, wg.name]

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
                        if " from " in description:
                            line_for_csv.append("to_clean")
                            li_for_csv.append(line_for_csv)
                            nb_per_round += 1
                            li_event_to_parse.append(("to_clean", event, md))
                        else:
                            pass
                        continue

                    else:
                        pass
            else:
                pass
        logger.info("{} corrupted events retrieved into '{}' worgroup's metadatas".format(nb_per_round, wg.name))

    coord_sys_prefix = " The coordinate system was changed from "
    attribute_type_prefix = " The length of the attribute attribute "
    attribute_length_prefix = " The type of the attribute attribute "
    attribute_infix = " has been changed from "

    nb_event_deleted = 0
    for tup in li_event_to_parse:
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
        event = Event(**tup[1])
        md = isogeo.metadata.get(tup[2].get("_id"))
        if tup[0] == "to_delete":
            isogeo.metadata.events.delete(event=event, metadata=md)
            nb_event_deleted += 1
        elif tup[0] == "to_clean":
            description = event.description

            li_lines = []
            for part in description.split("\r\n___\r\n"):
                li_items = part.split("\n*")
                for item in li_items:
                    li_lines.append(item)

            new_description = "The dataset has been modified :\n"

            deleted = 0
            for line in li_lines:
                if line.startswith(coord_sys_prefix):
                    step_index = line.index(" to ")
                    orig = line[len(coord_sys_prefix):step_index]
                    dest = line[step_index + 4:].replace("\n", "").replace(" \n", "")
                    if orig == dest or orig.replace("https", "http") == dest.replace("https", "http"):
                        pass
                    else:
                        new_description += "\n*{}".format(line)
                        pass

                elif (line.startswith(attribute_type_prefix) or line.startswith(attribute_length_prefix)) and attribute_infix in line:
                    step_index = line.index(" to ")
                    orig = line[line.index(attribute_infix) + len(attribute_infix):step_index]
                    dest = line[step_index + 4:].replace("\n", "").replace(" \n", "")
                    if orig == dest or "coded_domain(" in line:
                        pass
                    else:
                        new_description += "\n*{}".format(line)
                        pass

                elif line.strip() != "The dataset has been modified :":
                    new_description += "\n*{}".format(line)
                    pass

                else:
                    pass

            new_description = new_description.replace("undefined", "")

            if new_description.strip() == "The dataset has been modified :":
                isogeo.metadata.events.delete(event=event, metadata=md)
            else:
                event.description = new_description
                isogeo.metadata.events.update(event=event, metadata=md)

        else:
            logger.info("Unexpected event to parse type : {}".format(tup[0]))

    logger.info("{} event deleted".format(nb_event_deleted))

    isogeo.close()

    csv_path = Path(r"./scripts/misc/events/csv/corrupted_after_clean.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "event_uuid",
                "event_date",
                "event_description",
                "md_uuid",
                "wg_uuid",
                "wg_name",
                "to_do"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)