# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for Jura data in 2019
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
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
    # ApiCatalog
)

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

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
        Path("./scripts/jura/_logs/migration_jura.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n------------ MIGRATION SESSION ------------")
    logger.info("TESTING MAPPING TABLE")
    # ################# CHECK MAPPING TABLE and RETRIEVE UUID FROM IT #################
    # to store source metadata uuid to backup
    li_to_backup = []
    # to store source metadata uuid, title and name that passe the tests
    li_src_to_migrate = []
    # store all source uuid that appear in the mapping table
    src_found = []
    # to store target metadata uuid, title and name that passe the tests
    li_trg_to_migrate = []
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
            if src_uuid != "source_uuid":
                src_found.append(src_uuid)
                trg_found.append(trg_uuid)
                # check if the target metadata exists
                if trg_uuid == "NR":
                    logger.info("l.{} - there is no target".format(row_num))
                # check source UUID validity
                elif not checker.check_is_uuid(src_uuid):
                    logger.info(
                        "l.{} - {} source UUID isn't valid".format(row_num, src_uuid)
                    )
                # check if source UUID appears just one time in the field
                elif li_src_to_migrate.count(src_uuid) > 0:
                    logger.info(
                        "l.{} - {} already exist in the tab at line {}".format(
                            row_num, src_uuid, str(src_found.index(src_uuid) + 1)
                        )
                    )
                # if UUID, title and name of source metadata have passed all checks,
                # time to test UUID and nam of target metadata
                else:
                    # check target UUID validity
                    if not checker.check_is_uuid(trg_uuid):
                        logger.info(
                            "l.{} -{} target UUID isn't valid".format(row_num, trg_uuid)
                        )
                    # check if target UUID appears just one time in the field
                    elif li_trg_to_migrate.count(trg_uuid) > 0:
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
                    # if all check are passed, metadata are stored into a tuple that is
                    # added to a list, plus source and targt UUID are added to the list
                    # that will be used to backup
                    else:
                        to_migrate = (src_uuid, src_title, src_name)
                        li_src_to_migrate.append(to_migrate)
                        to_migrate = (trg_uuid, trg_name)
                        li_trg_to_migrate.append(to_migrate)

                        li_to_backup.append(src_uuid)
            else:
                pass

    # once each row have been test, a summary of the checks is logged
    expected_uuid_nb = len(src_found)
    found_uuid_nbr = len(li_src_to_migrate)
    if found_uuid_nbr == expected_uuid_nb:
        logger.info("--> All lines passed the check.")
    else:
        logger.info(
            "--> {}/{} lines didn't passe the check.".format(
                expected_uuid_nb - found_uuid_nbr, expected_uuid_nb
            )
        )
    if len(set(li_src_to_migrate)) == found_uuid_nbr:
        logger.info("--> Each source uuid appears only once.")
    else:
        logger.info("--> There is some duplicate source uuid.")

    found_uuid_nbr = len(li_trg_to_migrate)
    if len(set(li_trg_to_migrate)) == found_uuid_nbr:
        logger.info("--> Each target uuid appears only once.")
    else:
        logger.info("--> There is some duplicate target uuid.")

    # ############################### MIGRATING & SAVING ###############################
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

    logger.info(
        "{} medatatas will be migrated and {} will be saved".format(
            len(li_src_to_migrate),
            len(li_to_backup)
        )
    )
    # ------------------------------------- BACKUP -------------------------------------
    backup_mngr = BackupManager(api_client=isogeo, output_folder="./scripts/jura/output")

    bound_range = int(len(li_to_backup) / 50)
    li_bound = []
    for i in range(bound_range + 1):
        li_bound.append(50 * i)
    li_bound.append(len(li_to_backup))

    logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
    for i in range(len(li_bound) - 1):
        bound_inf = li_bound[i]
        bound_sup = li_bound[i + 1]
        logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

        search_parameters = {"query": None, "specific_md": tuple(li_to_backup[bound_inf:bound_sup])}
        try:
            backup_mngr.metadata(search_params=search_parameters)
        except Exception as e:
            logger.info("an error occured : {}".format(e))

    # ----------------------------------- MIGRATING ------------------------------------
    logger.info("Starting migration")
    li_migrated = []
    li_failed = []
    index = 0
    for md in li_src_to_migrate:
        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_src_to_migrate)))

        src_uuid = md[0]
        src_title = md[1]
        src_name = md[2]
        trg_uuid = li_trg_to_migrate[index][0]
        trg_name = li_trg_to_migrate[index][1]

        # loading the metadata to duplicate from his UUID
        try:
            src_migrator = MetadataDuplicator(
                api_client=isogeo, source_metadata_uuid=src_uuid
            )
            src_loaded = src_migrator.metadata_source
        except Exception as e:
            logger.info("Faile to load {} source metadata : \n {}".format(src_uuid, e))
            li_failed.append(
                [
                    src_uuid,
                    src_title,
                    src_name,
                    trg_name,
                    trg_uuid
                ]
            )
            index += 1
            continue

        # check if the metadata exists
        if isinstance(src_loaded, tuple):
            logger.info(
                "{} - There is no accessible source metadata corresponding to this "
                "uuid".format(src_uuid)
            )
            pass

        # checks metadata name and title indicated in the mapping table
        # then, dupplicate the metadata
        else:
            li_exclude_fields = [
                "coordinateSystem",
                "envelope",
                "features",
                "geometry",
                "name",
                "path",
                "format",
                "formatVersion",
                "series",
            ]
            try:
                md_dst = src_migrator.import_into_other_metadata(
                    copymark_abstract=False,  # FALSE EN PROD
                    copymark_title=False,  # FALSE EN PROD
                    destination_metadata_uuid=trg_uuid,
                    exclude_fields=li_exclude_fields,
                )
                li_migrated.append(
                    [
                        src_loaded._id,
                        src_loaded.title,
                        src_loaded.name,
                        md_dst.name,
                        md_dst._id,
                    ]
                )
            except Exception as e:
                logger.info("Failed to import {} into {} : \n {}".format(src_uuid, trg_uuid, e))
                li_failed.append(
                    [
                        src_uuid,
                        src_title,
                        src_name,
                        trg_name,
                        trg_uuid
                    ]
                )
                index += 1
                continue
            index += 1

    isogeo.close()

    csv_result = Path(r"./scripts/jura/migrated.csv")
    with open(csv_result, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
            ]
        )
        for data in li_migrated:
            writer.writerow(data)

    if len(li_failed) > 0:
        logger.info("{} metadatas haven't been migrated. Launch the script again pointing to 'migrate_failed.csv' file".format(len(li_failed)))
        csv_failed = Path(r"./scripts/jura/migrate_failed.csv")
        with open(csv_failed, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(
                [
                    "source_uuid",
                    "source_title",
                    "source_name",
                    "target_name",
                    "target_uuid",
                ]
            )
            for data in li_failed:
                writer.writerow(data)
    else:
        logger.info("All metadatas have been migrated ! :)")
