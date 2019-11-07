# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Duplicate script for Jura data in 2019
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load .env file
load_dotenv(".env", override=True)

checker = IsogeoChecker()

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
        Path("./scripts/jura/duplicate_jura.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n------------ DUPLICATING SESSION ------------")
    logger.info("TESTING MAPPING TABLE")
    # ############# CHECK MAPPING TABLE #############
    # to store source metadata uuid, title and name that passe the tests
    li_src_to_duplicate = []
    # store all source uuid that appear in the mapping table
    src_found = []
    # to store target metadata uuid, title and name that passe the tests
    li_trg_to_duplicate = []
    # store all target uuid that appear in the mapping table
    trg_found = []
    # prepare csv reading
    input_csv = Path(r"./scripts/jura/correspondances.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        row_num = 0
        for row in reader:
            row_num += 1
            src_uuid = row.get("source_uuid")
            src_title = row.get("source_title")
            src_name = row.get("source_name")
            trg_uuid = row.get("target_uuid")
            trg_name = row.get("target_name")
            # *=====* to duplicate only 1 on 10 sheets
            if src_uuid != "source_uuid" and row_num % 25 == 0:
                src_found.append(src_uuid)
                trg_found.append(trg_uuid)
                # check source UUID validity
                if trg_uuid == "NR":
                    logger.info("l.{} - there is no target".format(row_num))
                elif not checker.check_is_uuid(src_uuid):
                    logger.info(
                        "l.{} - {} is not a regular UUID".format(row_num, src_uuid)
                    )
                # check if source UUID appears just one time in the field
                elif li_src_to_duplicate.count(src_uuid) > 0:
                    logger.info(
                        "l.{} - {} already exist in the tab at line {}".format(
                            row_num, src_uuid, str(src_found.index(src_uuid) + 1)
                        )
                    )
                # if all check are passed : uuid, title and name of source metadata
                # are stored into tuple and added to a list
                else:
                    # check target UUID validity
                    if not checker.check_is_uuid(trg_uuid):
                        logger.info(
                            "l.{} -{} target UUID isn't valid".format(row_num, trg_uuid)
                        )
                    # check if target UUID appears just one time in the field
                    elif li_trg_to_duplicate.count(trg_uuid) > 0:
                        logger.info(
                            "l.{} - {} target UUID already exist in the tab at line {}".format(
                                row_num, trg_uuid, str(trg_found.index(trg_uuid) + 1)
                            )
                        )
                    # check if target UUID is different from source UUID
                    elif trg_uuid == src_uuid:
                        logger.info(
                            "l.{} - {} target and source UUID are the same".format(
                                row_num, trg_uuid
                            )
                        )
                    # if all check are passed : uuid, and name of target and source
                    # metadata are stored into a tuple and added to a list
                    else:
                        to_duplicate = (src_uuid, src_title, src_name)
                        li_src_to_duplicate.append(to_duplicate)
                        to_duplicate = (trg_uuid, trg_name)
                        li_trg_to_duplicate.append(to_duplicate)
            else:
                pass

    # once each row have been test, the results of the checks are displayed in the log
    expected_uuid_nb = len(src_found)
    found_uuid_nbr = len(li_src_to_duplicate)
    if found_uuid_nbr == expected_uuid_nb:
        logger.info("--> All lines passed the check.")
    else:
        logger.error(
            "--> {}/{} lines didn't passe the check.".format(
                expected_uuid_nb - found_uuid_nbr, expected_uuid_nb
            )
        )
    if len(set(li_src_to_duplicate)) == found_uuid_nbr:
        logger.info("--> Each source uuid appears only once.")
    else:
        logger.error("--> There is some duplicate source uuid.")

    found_uuid_nbr = len(li_trg_to_duplicate)
    if len(set(li_trg_to_duplicate)) == found_uuid_nbr:
        logger.info("--> Each target uuid appears only once.")
    else:
        logger.error("--> There is some duplicate target uuid.")

    logger.info(
        "DUPLICATING {} METADATAS INTO 'ISOGEO MIGRATIONS' WORKGROUP".format(
            len(li_src_to_duplicate) * 2
        )
    )
    # ############# DUPLICATING #############
    # API client instanciation
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
        auth_mode="user_legacy",
    )
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )
    # to build mapping table of 'Isogeo Migrations' workgroup
    li_migrated = []
    index = 0
    for md in li_src_to_duplicate:
        src_uuid = md[0]
        src_title = md[1]
        src_name = md[2]
        trg_uuid = li_trg_to_duplicate[index][0]
        trg_name = li_trg_to_duplicate[index][1]

        # loading the metadata to duplicate from his UUID
        src_migrator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=src_uuid
        )
        src_loaded = src_migrator.metadata_source

        trg_migrator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=trg_uuid
        )
        trg_loaded = trg_migrator.metadata_source

        # check if the metadata exists
        if isinstance(src_loaded, tuple):
            logger.info(
                "{} - There is no accessible source metadata corresponding to this "
                "uuid".format(src_uuid)
            )
            pass
        elif isinstance(trg_loaded, tuple):
            logger.info(
                "{} - There is no accessible target metadata corresponding to this "
                "uuid".format(trg_uuid)
            )
            pass
        # checks metadata name and title indicated in the mapping table
        # then, dupplicate the metadata
        else:
            if src_title == src_loaded._title:
                pass
            else:
                logger.info(
                    "{} - this metadata title is '{}' and not '{}'".format(
                        src_uuid, src_loaded._title, src_title
                    )
                )
                pass
            if src_name == src_loaded._name:
                pass
            else:
                logger.info(
                    "{} - this metadata name is '{}' and not '{}'".format(
                        src_uuid, src_loaded._name, src_name
                    )
                )
                pass

            if trg_name == src_loaded._name:
                pass
            else:
                logger.info(
                    "{} - this metadata name is '{}' and not '{}'".format(
                        trg_uuid, src_loaded._name, trg_name
                    )
                )
                pass
            src_migrated = src_migrator.duplicate_into_other_group(
                destination_workgroup_uuid=environ.get("ISOGEO_WORKGROUP_MIGRATION"),
                copymark_abstract=True,
                copymark_title=True,
            )
            trg_migrated = trg_migrator.duplicate_into_other_group(
                destination_workgroup_uuid=environ.get("ISOGEO_WORKGROUP_MIGRATION"),
                copymark_abstract=True,
                copymark_title=True,
            )
            li_migrated.append(
                [
                    src_migrated._id,
                    src_migrated.title,
                    src_migrated.name,
                    trg_migrated._id,
                    trg_migrated.name,
                ]
            )
            index += 1
    isogeo.close()

    csv_sample = Path(r"./scripts/jura/sample.csv")
    with open(csv_sample, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_uuid",
                "target_name",
            ]
        )
        for data in li_migrated:
            writer.writerow(data)
