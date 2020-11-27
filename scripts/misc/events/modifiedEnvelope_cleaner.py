# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to clean modified envelope related corrupted events added by Isogeo Scan
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
        print("\n")


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
        Path("./scripts/misc/events/_logs/envelopeModified_events_cleaner.log"), "a", 5000000, 1
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
    input_csv = Path(r"./scripts/misc/events/csv/corrupted_v6.csv")
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
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            wg_name = row.get("wg_name")
            wg_uuid = row.get("wg_uuid")
            md_uuid = row.get("md_uuid")
            event_uuid = row.get("event_uuid")
            issue = row.get("issue")
            if issue == "envelopeModified":
                li_events_to_clean.append((wg_name, wg_uuid, md_uuid, event_uuid, issue))
            else:
                pass
    nb_to_parse = len(li_events_to_clean)

    li_dataModified_labels = ["La donnée a été modifiée :", "The dataset has been modified :"]
    envelopeModified_label_fr = "L’enveloppe a été modifiée"

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
    nb_deleted = 0

    current_md_uuid = ""
    li_for_csv = []
    for tup in li_events_to_clean:
        nb_parsed += 1
        printProgressBar(
            iteration=nb_parsed,
            total=nb_to_parse,
            prefix='Processing progress:',
            length=100,
            suffix="- {} deleted / {} updated".format(nb_deleted, nb_updated)
        )
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
        if envelopeModified_label_fr in event.description:
            part_count = 0
            new_description = ""
            # browsing different parts of event description
            for part in event.description.split("\r\n___\r\n"):
                part_count += 1
                # browsing differents items of the current part
                for item in part.split("\n*"):
                    if item.strip() == "":
                        continue
                    # automatically adding the header of the current part to the new description
                    if li_dataModified_labels[1] in item or li_dataModified_labels[0] in item:
                        new_description += item
                        continue
                    # adding the bullet point if the current item is not the header
                    else:
                        new_description += "\n*"
                    # if the current item is related to modified envelope, let's check if it is corrupted
                    if item.strip() == envelopeModified_label_fr:
                        if new_description.endswith("*"):
                            new_description = new_description[:-1]
                        else:
                            pass
                    # if the current is not related to modified envelope, let's add it to the new description
                    else:
                        new_description += item

                # add the separation between parts only if the current one is not the last of the description
                if part_count < len(event.description.split("\r\n___\r\n")):
                    new_description += "\r\n___\r\n"
                else:
                    pass
            # check what's left after removing headers, bullet points and parts separations
            # if there is nothing left, it means that the description contained only corrupted content
            description_light = new_description.replace("___", "").replace("* ", "").replace(li_dataModified_labels[1], "").replace(li_dataModified_labels[0], "").replace(envelopeModified_label_fr, "").strip()
            # so delete the event
            if description_light == "":
                li_for_csv.append(
                    [
                        tup[0],
                        tup[1],
                        md._id,
                        event._id,
                        event.description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                        new_description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                        "deleted",
                    ]
                )
                if int(environ.get("HARD_MODE")):
                    isogeo.metadata.events.delete(event=event, metadata=md)
                else:
                    pass
                nb_deleted += 1
            # otherwise the event is updated with the new description from which corrupted content has been removed
            else:
                # new_description = new_description.strip()
                # if li_dataModified_labels[0] in new_description or li_dataModified_labels[1] in new_description:
                #     label = [lbl for lbl in li_dataModified_labels if lbl in new_description][0]
                #     if new_description.startswith(label):
                #         li_items_cleaned = [""]
                #     else:
                #         li_items_cleaned = []
                #     for item in new_description.split(label):
                #         if item.strip() == "" or item.strip() == "___":
                #             pass
                #         else:
                #             li_items_cleaned.append(item)
                #     new_description = label.join(li_items_cleaned)
                # else:
                #     pass
                # if new_description.strip().endswith("___"):
                #     new_description = new_description.strip()[:-3].strip()
                # else:
                #     pass
                # li_for_csv.append(
                #     [
                #         tup[0],
                #         tup[1],
                #         md._id,
                #         event._id,
                #         event.description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                #         new_description.replace("\n", "\\n").replace("\r", "\\r").replace(";", "<point-virgule>"),
                #         "cleaned",
                #     ]
                # )
                # event.description = new_description
                # if int(environ.get("HARD_MODE")):
                #     isogeo.metadata.events.update(event=event, metadata=md)
                # else:
                #     pass
                # nb_updated += 1
                continue
        else:
            continue

    isogeo.close()

    logger.info("--> {} corrupted events have been processed including :".format(nb_parsed))
    logger.info("  - {} updated".format(nb_updated))
    logger.info("  - {} deleted".format(nb_deleted))
    logger.info("  - {} skipped\n".format(nb_to_parse - (nb_updated + nb_deleted)))

    csv_path = Path("./scripts/misc/events/csv/modifiedEnvelop_cleaner_{}.csv".format(str(datetime.now().timestamp()).split(".")[0]))
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "wg_name",
                "wg_uuid",
                "md_uuid",
                "event_uuid",
                "event_description",
                "event_description_light",
                "operation",
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
