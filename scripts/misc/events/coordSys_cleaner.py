# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to clean coordinate system related corrupted events added by Isogeo Scan
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
from isogeo_pysdk import Isogeo, IsogeoChecker

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
        Path("./scripts/misc/events/_logs/coordSys_events_cleaner.log"), "a", 5000000, 1
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
        "issue",
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
            if issue == "coordSys":
                li_events_to_clean.append((wg_name, wg_uuid, md_uuid, event_uuid))
            else:
                pass

    dataModified_label_fr = "La donnée a été modifiée :"
    dataModified_label_en = "The dataset has been modified :"
    coordSys_prefix_en = (" The coordinate system was changed from ",)
    coordSys_infix_en = " to "

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

        if "coordinate system" in event.description:
            part_count = 0
            new_description = ""
            # browsing different parts of event description
            for part in event.description.split("\r\n___\r\n"):
                part_count += 1
                # browsing differents items of the current part
                for item in part.split("\n*"):
                    # automatically adding the header of the current part to the new description
                    if dataModified_label_en in item or dataModified_label_fr in item:
                        new_description += item
                        continue
                    # adding the bullet point if the current item is not the header
                    else:
                        new_description += "\n*"
                    # if the current item is related to coordinate system, let's check if it is corrupted
                    if "coordinate system" in item:
                        value1 = item[
                            len(coordSys_prefix_en):item.index(coordSys_infix_en)
                        ].strip().replace("https", "http")
                        value2 = item[
                            item.index(coordSys_infix_en) + len(coordSys_infix_en):
                        ].strip().replace("https", "http")
                        # just removing previously added bullet point if it's corrupted
                        if value1 == value2:
                            # remove the bullet point
                            new_description = new_description[:-3]
                        # adding it to the new description if it's not
                        else:
                            new_description += item
                    # if the current is not related to coordinate system, let's add it to the new description
                    else:
                        new_description += item

                # add the separation between parts only if the current one is not the last of the description
                if part_count < len(event.description.split("\r\n___\r\n")):
                    new_description += "\r\n___\r\n"
                else:
                    pass
            # check what's left after removing headers, bullet points and parts separations
            # if there is nothing left, it means that the description contained only corrupted content
            description_light = (
                new_description.replace("___", "")
                .replace("*", "")
                .replace(dataModified_label_en, "")
                .replace(dataModified_label_fr, "")
                .strip()
            )
            # so delete the event
            if description_light == "":
                li_for_csv.append(
                    [
                        md._id,
                        event._id,
                        event.description.replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace(";", "<point-virgule>"),
                        new_description.replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace(";", "<point-virgule>"),
                        "to_delete",
                    ]
                )
                # isogeo.metadata.events.delete(event=event, metadata=md)
                pass
            # otherwise the event is updated with the new description from which corrupted content has been removed
            else:
                if new_description.strip().endswith(dataModified_label_en):
                    new_description = new_description.strip()[
                        : -len(dataModified_label_en)
                    ]
                elif new_description.strip().endswith(dataModified_label_fr):
                    new_description = new_description.strip()[
                        : -len(dataModified_label_fr)
                    ]
                else:
                    new_description = new_description.strip()
                li_for_csv.append(
                    [
                        md._id,
                        event._id,
                        event.description.replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace(";", "<point-virgule>"),
                        new_description.replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace(";", "<point-virgule>"),
                        "to_clean",
                    ]
                )
                event.description = new_description
                # isogeo.metadata.events.update(event=event, metadata=md)
                pass
        else:
            continue

    isogeo.close()

    csv_path = Path(r"./scripts/misc/events/csv/coordSys_cleaner.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "md_uuid",
                "event_uuid",
                "event_description",
                "event_description_light",
                "to_do",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
