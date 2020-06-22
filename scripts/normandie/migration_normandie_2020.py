# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for Normandie data in 2020
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
from timeit import default_timer
import datetime

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

# load dijon.env file
load_dotenv("env/normandie.env", override=True)

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
        Path("./scripts/normandie/_logs/migration_normandie.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logger.info("\n######################## MIGRATION SESSION ########################")
    logger.info("-------------- RETRIEVING INFOS FROM MAPPING TABLE ------------------")

    # ################# CHECK MAPPING TABLE and RETRIEVE UUID FROM IT #################
    # to metadata uuid tuples that gonna be migrates
    li_to_migrate = []
    li_uuid_1 = []
    li_uuid_2 = []
    # store all source and target metadata uuid
    li_to_backup = []
    # prepare csv reading
    input_csv = Path(r"./scripts/normandie/csv/mapping.csv")
    fieldnames = [
        "data_name",
        "md_uuid_1",
        "md_uuid_2",
        "match_type"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        row_num = 0
        for row in reader:
            row_num += 1
            data_name = row.get("source_uuid")
            md_uuid_1 = row.get("source_title")
            md_uuid_2 = row.get("source_name")
            match_type = row.get("target_name")
            if data_name != "data_name":
                # check if the target metadata exists
                if md_uuid_2 == "no_match":
                    logger.info("l.{} - there is no target".format(row_num))
                # check source UUID validity
                elif not checker.check_is_uuid(md_uuid_1):
                    logger.info(
                        "l.{} - {} source UUID isn't valid".format(row_num, md_uuid_1)
                    )
                # check if source UUID appears just one time in the field
                elif li_uuid_1.count(md_uuid_1) > 0:
                    logger.info(
                        "l.{} - {} already exist in the tab at line {}".format(
                            row_num, md_uuid_1, str(li_uuid_1.index(md_uuid_1) + 1)
                        )
                    )
                # if UUID, title and name of source metadata have passed all checks,
                # time to test UUID and nam of target metadata
                else:
                    # check target UUID validity
                    if not checker.check_is_uuid(md_uuid_2):
                        logger.info(
                            "l.{} - {} source UUID isn't valid".format(row_num, md_uuid_2)
                        )
                    # check if target UUID appears just one time in the field
                    elif li_uuid_2.count(md_uuid_2) > 0:
                        logger.info(
                            "l.{} - {} already exist in the tab at line {}".format(
                                row_num, md_uuid_2, str(li_uuid_2.index(md_uuid_2) + 1)
                            )
                        )
                    # check if target UUID is different from source UUID
                    elif md_uuid_1 == md_uuid_2:
                        logger.info(
                            "l.{} - {} target and source UUID are the same".format(
                                row_num, md_uuid_2
                            )
                        )
                    # if all check are passed, metadata are stored into a tuple that is
                    # added to a list
                    else:
                        to_migrate = (md_uuid_1, md_uuid_2, match_type, data_name)
                        li_to_migrate.append(to_migrate)

                        li_to_backup.append(md_uuid_1)
                        li_to_backup.append(md_uuid_2)

                        li_uuid_1.append(md_uuid_1)
                        li_uuid_2.append(md_uuid_2)
            else:
                pass

    # ############################### MIGRATING ###############################
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

    logger.info(
        "{} metadatas will be migrated".format(
            len(li_to_migrate)
        )
    )

    # ------------------------------------ BACKUP --------------------------------------
    if environ.get("BACKUP") == "1":
        logger.info("---------------------------- BACKUP ---------------------------------")
        # backup manager instanciation
        backup_path = Path(r"./scripts/normandie/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_to_backup) > amplitude:
            bound_range = int(len(li_to_backup) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_to_backup))

            logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
            for i in range(len(li_bound) - 1):
                if default_timer() - auth_timer >= 250:
                    logger.info("Manually refreshing token")
                    backup_mng.isogeo.connect(
                        username=environ.get("ISOGEO_USER_NAME"),
                        password=environ.get("ISOGEO_USER_PASSWORD"),
                    )
                    auth_timer = default_timer()
                else:
                    pass
                bound_inf = li_bound[i]
                bound_sup = li_bound[i + 1]
                logger.info("Round {} - backup from source metadata {} to {}".format(i + 1, bound_inf + 1, bound_sup))

                search_parameters = {"query": None, "specific_md": tuple(li_to_backup[bound_inf:bound_sup])}
                try:
                    backup_mng.metadata(search_params=search_parameters)
                except Exception as e:
                    logger.info("an error occured : {}".format(e))
        else:
            search_parameters = {"query": None, "specific_md": tuple(li_to_backup)}
            backup_mng.metadata(search_params=search_parameters)
    else:
        pass

    # ----------------------------------- MIGRATING ------------------------------------
    logger.info("--------------------------- MIGRATING -------------------------------")
    li_migrated = []
    li_failed = []
    index = 0
    for tup in li_to_migrate:
        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_to_migrate)))
        uuid_1 = tup[0]
        uuid_2 = tup[1]
        match_type = tup[2]

        if default_timer() - auth_timer >= 230:
            logger.info("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        md_1 = isogeo.metadata.get(uuid_1)
        md_2 = isogeo.metadata.get(uuid_2)

        if match_type == "src_matching":
            # parse Metadata._created attribute to retrieve
            str_date = md_1._created.split("T")[0]
            creation_date_1 = datetime.datetime.strptime(str_date, "%Y-%m-%d")
            str_date = md_2._created.split("T")[0]
            creation_date_2 = datetime.datetime.strptime(str_date, "%Y-%m-%d")
            # let's retrieve the older metadata supposed to be the source
            if creation_date_1 < creation_date_2:
                src_md = md_1
                trg_md = md_2
                src_uuid = md_1._id
                trg_uuid = md_2._id
            else:
                src_md = md_2
                trg_md = md_1
                src_uuid = md_2._id
                trg_uuid = md_1._id
            # loading source metadata using MetadataDuplicator
            try:
                src_migrator = MetadataDuplicator(
                    api_client=isogeo, source_metadata_uuid=src_uuid
                )
                src_loaded = src_migrator.metadata_source
            except Exception as e:
                logger.info("Faile to load {} source metadata : \n {}".format(src_uuid, e))
                li_failed.append(
                    [
                        src_md._id,
                        src_md.title,
                        src_md.name,
                        trg_dst.name,
                        trg_md._id,
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

            # let's dupplicate the metadata
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
                        copymark_catalog=environ.get("MIGRATED_CAT_UUID"),
                        destination_metadata_uuid=trg_uuid,
                        exclude_fields=li_exclude_fields,
                        switch_service_layers=True
                    )
                    li_migrated.append(
                        [
                            src_loaded._id,
                            src_loaded.title,
                            src_loaded.name,
                            trg_md.name,
                            trg_md._id,
                        ]
                    )
                except Exception as e:
                    logger.info("Failed to import {} into {} : \n {}".format(src_uuid, trg_uuid, e))
                    li_failed.append(
                        [
                            src_loaded._id,
                            src_loaded.title,
                            src_loaded.name,
                            trg_md.name,
                            trg_md._id,
                        ]
                    )
                    index += 1
                    continue
                index += 1

        elif match_type == "trg_matching":
            sign_1 = md_1.signature()
            sign_2 = md_2.signature()
            if sign_1 == sign_2:
                isogeo.metadata.delete(md_1._id)
            else:
                logger.info("{} and {} metadata are supposed to be same but their signature values are different".format(md_1._id, md_2._id))

        else:
            logger.info("Unexpected match type : {}".format(match_type))

    isogeo.close()

    csv_result = Path(r"./scripts/normandie/csv/migrated.csv")
    with open(csv_result, "w", newline="") as csvfile:
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
        csv_failed = Path(r"./scripts/normandie/csv/migrate_failed.csv")
        with open(csv_failed, "w", newline="") as csvfile:
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