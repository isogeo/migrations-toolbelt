# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to list corrupted events due to scan refactory and write the result as a csv file
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
from datetime import datetime, timezone

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo
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
        Path("./scripts/misc/events/_logs/corrupted_events_reporter.log"),
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

    dataModified_label_fr = "La donnée a été modifiée :"
    dataModified_label_en = "The dataset has been modified :"
    envelopeModified_label_fr = "L’enveloppe a été modifiée"

    li_pattern = [
        {
            "name": "coordSys",
            "prefix": " The coordinate system was changed from ",
            "infix1": " to ",
        },
        {
            "name": "dataPath_fr",
            "prefix": " L’emplacement de la donnée a été modifié de ",
            "infix1": " à ",
        },
        {
            "name": "dataPath_en",
            "prefix": " The data path has been modified from ",
            "infix1": " to ",
        },
        {
            "name": "attributeType",
            "prefix": " The type of the attribute ",
            "infix1": " has been changed from ",
            "infix2": " to ",
        },
        {
            "name": "attributeLength",
            "prefix": " The length of the attribute ",
            "infix1": " has been changed from ",
            "infix2": " to ",
        },
        {
            "name": "attributePrecision",
            "prefix": " The precision of the attribute ",
            "infix1": " has been changed from ",
            "infix2": " to ",
        },
        {
            "name": "attributeScale",
            "prefix": " The scale of the attribute ",
            "infix1": " has been changed from ",
            "infix2": " to ",
        },
    ]
    li_pattern_prefix = [pattern.get("prefix") for pattern in li_pattern]

    bound_date = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)

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

    li_wg_uuid = environ.get("ISOGEO_INVOLVED_WORKGROUPS").split(";")
    li_wg = [isogeo.workgroup.get(wg_uuid) for wg_uuid in li_wg_uuid]

    logger.info("{} workgroups gonna be inspected\n".format(len(li_wg_uuid)))

    li_involved_wg = []
    li_for_csv = []
    li_event_to_parse = []
    # First, let inspected workgroups looking for metadatas with events that need to be cleaned.
    for wg in li_wg:
        nb_per_round = 0
        logger.info("{}/{} - Inspecting '{}' workgroup ({})".format(li_wg.index(wg) + 1, len(li_wg), wg.name, wg._id))

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

        # Retrieve all workgroup's metadatas
        wg_search = isogeo.search(group=wg._id, whole_results=True, include=("events",))
        logger.info(
            "   {} metadatas retrieved from '{}' workgroup".format(
                wg_search.total, wg.name
            )
        )

        wg_md = wg_search.results
        for md in wg_md:
            if len(md.get("events")):
                # Only retreving "update" event which description is not empty and published before april 2020
                md_events = [event for event in md.get("events") if event.get("description") and event.get("kind") == "update" and datetime.fromisoformat(event.get("date")) > bound_date]
                for event in md_events:
                    description = event.get("description")
                    line_for_csv = [
                        wg.name,
                        wg._id,
                        md.get("_id"),
                        event.get("_id"),
                        event.get("date"),
                        description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>").replace("|", "<pipe>"),
                    ]

                    if "undefined" in description:
                        nb_per_round += 1
                        line_for_csv.append("undefined")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif description.startswith("eventDescription"):
                        nb_per_round += 1
                        line_for_csv.append("eventDescription")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif " attribute attribute " in description:
                        nb_per_round += 1
                        line_for_csv.append(" attribute attribute ")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif description.strip() in dataModified_label_en or description.strip() in dataModified_label_fr:
                        nb_per_round += 1
                        line_for_csv.append("empty")
                        li_for_csv.append(line_for_csv)
                        continue
                    elif envelopeModified_label_fr in description:
                        description_light = description.replace("___", "").replace("* ", "").replace(dataModified_label_fr, "").replace(dataModified_label_en, "").replace(envelopeModified_label_fr, "").strip()
                        if description_light == "":
                            nb_per_round += 1
                            line_for_csv.append("envelopeModified")
                            li_for_csv.append(line_for_csv)
                            continue
                        else:
                            pass
                    else:
                        pass

                    if description.startswith(dataModified_label_en) or description.startswith(dataModified_label_fr):
                        li_item = []
                        for part in description.split("\r\n___\r\n"):
                            for item in part.split("\n*"):
                                li_item.append(item)
                        for item in li_item:
                            item_pattern = [pattern for pattern in li_pattern if item.startswith(pattern.get("prefix"))]
                            if len(item_pattern):
                                item_pattern = item_pattern[0]
                                prefix = item_pattern.get("prefix")
                                infix1 = item_pattern.get("infix1")
                                infix2 = item_pattern.get("infix2")
                                if infix2:
                                    value1 = item[item.index(infix1) + len(infix1):item.index(infix2)].strip()
                                    value2 = item[item.index(infix2) + len(infix2):].strip()
                                else:
                                    value1 = item[len(prefix):item.index(infix1)].strip().replace("https", "http")
                                    value2 = item[item.index(infix1) + len(infix1):].strip().replace("https", "http")
                                if value1 == value2 or (item_pattern.get("name").startswith("dataPath") and value2 == "**.**"):
                                    line_for_csv.append(item_pattern.get("name"))
                                    li_for_csv.append(line_for_csv)
                                    nb_per_round += 1
                                    break
                                else:
                                    continue
                        if envelopeModified_label_fr in description:
                            description_light = description.replace("___", "").replace("* ", "").replace(dataModified_label_fr, "").replace(dataModified_label_en, "").replace(envelopeModified_label_fr, "").strip()
                            if description_light == "":
                                nb_per_round += 1
                                line_for_csv.append("envelopeModified")
                                li_for_csv.append(line_for_csv)
                                break
                            else:
                                continue
                        else:
                            continue
                    else:
                        pass
            else:
                pass
        logger.info(
            "   > {} corrupted events retrieved into '{}' worgroup's metadatas\n".format(
                nb_per_round, wg.name
            )
        )
        if nb_per_round > 0:
            li_involved_wg.append(wg._id)
        else:
            pass

    logger.info(
        "--> {} corrupted events retrieved into {} of the {} inspected worgroups\n".format(
            len(li_for_csv), len(li_involved_wg), len(li_wg_uuid)
        )
    )

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
