# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for "Métropole Européenne de Lille" workgroup metadata in 2020
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
from datetime import datetime

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

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

load_dotenv("env/lille.env", override=True)

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
        Path("./scripts/lille/_logs/migration_lille.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.DEBUG)
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
    # to source and target related informations for migration purpose
    li_to_migrate = []
    # store all source or target uuid that appear in the mapping table
    li_src_found = []
    li_trg_found = []
    # to store duplicated source or target uuid
    li_duplicate_src = []
    li_duplicate_trg = []
    # to store all source and target metadata uuid
    li_to_backup = []

    # prepare csv reading
    input_csv = Path(r"./scripts/lille/csv/correspondances_v3.csv")
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
        "match_type",
        "match_count"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_uuid = row.get("source_uuid")
            src_title = row.get("source_title")
            src_name = row.get("source_name")
            trg_name = row.get("target_name")
            trg_uuid = row.get("target_uuid")

            if reader.line_num > 1:
                li_src_found.append(src_uuid)
                li_trg_found.append(trg_uuid)
                # check if the target metadata exists
                if trg_uuid == "NR" or src_name != trg_name:
                    continue
                # check source UUID validity
                elif not checker.check_is_uuid(src_uuid):
                    logger.info(
                        "l.{} - {} source UUID isn't valid".format(reader.line_num, src_uuid)
                    )
                # check if source UUID appears just one time in the field
                elif li_src_found.count(src_uuid) > 1:
                    logger.info(
                        "l.{} - {} already exist in the matching table at line {}".format(
                            reader.line_num, src_uuid, str(li_src_found.index(src_uuid) + 2)
                        )
                    )
                    if src_uuid not in li_duplicate_src:
                        li_duplicate_src.append(src_uuid)
                    else:
                        pass
                # if UUID, title and name of source metadata have passed all checks,
                # time to test UUID and nam of target metadata
                else:
                    # check target UUID validity
                    if not checker.check_is_uuid(trg_uuid):
                        logger.info(
                            "l.{} - {} target UUID isn't valid".format(reader.line_num, trg_uuid)
                        )
                    # check if target UUID appears just one time in the field
                    elif li_trg_found.count(trg_uuid) > 1:
                        logger.info(
                            "l.{} - {} target UUID already exist in the matching table at line {}".format(
                                reader.line_num, trg_uuid, str(li_trg_found.index(trg_uuid) + 2)
                            )
                        )
                        if trg_uuid not in li_duplicate_trg:
                            li_duplicate_trg.append(trg_uuid)
                        else:
                            pass
                    # check if target UUID is different from source UUID
                    elif trg_uuid == src_uuid:
                        logger.info(
                            "l.{} - {} target and source UUID are the same".format(
                                reader.line_num, trg_uuid
                            )
                        )
                    # if all check are passed, metadata are stored into a tuple that is
                    # added to a list
                    else:
                        li_to_migrate.append(
                            (src_uuid, src_title, src_name, trg_uuid, trg_name)
                        )

                        li_to_backup.extend(
                            [src_uuid, trg_uuid]
                        )
            else:
                pass

    # once each row have been test, a summary of the checks is logged
    expected_uuid_nb = len(li_src_found)
    found_uuid_nbr = len(li_to_migrate)
    if found_uuid_nbr == expected_uuid_nb:
        logger.info("--> All lines passed the check.")
    else:
        logger.info(
            "--> {}/{} lines didn't passe the check.".format(
                expected_uuid_nb - found_uuid_nbr, expected_uuid_nb
            )
        )
    # looking for duplicate targets
    if len(li_duplicate_trg):
        logger.warning("--> There is some duplicate target uuid. Before proceeding further, you must choose:")
        for uuid in li_duplicate_trg:
            logger.warning(
                "- which of the source metadatas gonna be migrated into '{}' target metadata.".format(uuid)
            )
        logger.warning(
            "by deleting from the matching table the lines corresponding to the source records that will not be retained.".format()
        )
        exit()
    else:
        pass

    # looking for duplicate sources
    if len(li_duplicate_src):
        logger.warning("--> There is some duplicate source uuid.")
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
        "==> {} metadatas will be migrated".format(
            len(li_to_migrate)
        )
    )

    # ------------------------------------ BACKUP --------------------------------------
    if int(environ.get("BACKUP")) and len(li_to_backup):
        logger.info("---------------------------- BACKUP ---------------------------------")
        # backup manager instanciation
        backup_path = Path(r"./scripts/lille/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_to_backup) > amplitude:
            bound_range = int(len(li_to_backup) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_to_backup))

            logger.info("Starting backup for {} rounds ({} metadatas gonna be backuped)".format(len(li_bound) - 1, len(li_to_backup)))
            for i in range(len(li_bound) - 1):
                if default_timer() - auth_timer >= 6900:
                    logger.debug("Manually refreshing token")
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
    li_cat_to_exclude = []
    if environ.get("ISOGEO_CATALOG_SOURCE"):
        li_cat_to_exclude.append(environ.get("ISOGEO_CATALOG_SOURCE"))

    li_migrated = []
    li_failed = []
    index = 0
    for to_migrate in li_to_migrate:
        # inform the user about processing progress
        # printProgressBar(
        #     iteration=index + 1,
        #     total=len(li_to_migrate),
        #     prefix='Processing progress:',
        #     length=100,
        #     suffix="- {}/{} metadata migrated".format(index + 1, len(li_to_migrate))
        # )

        # refresh token if needed
        if default_timer() - auth_timer >= 230:
            logger.debug("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        logger.info("------- Migrating metadata {}/{} -------".format(index + 1, len(li_to_migrate)))

        src_uuid = to_migrate[0]
        src_title = to_migrate[1]
        src_name = to_migrate[2]
        trg_uuid = to_migrate[3]
        trg_name = to_migrate[4]

        # check if target metadata have already been migrated
        md_dst_cat = isogeo.catalog.metadata(metadata_id=trg_uuid)
        md_dst_cat_uuid = [cat.get("_id") for cat in md_dst_cat]
        if environ.get("ISOGEO_CATALOG_MIGRATED") in md_dst_cat_uuid:
            logger.info("'{}' target metadata has already been migrated".format(trg_uuid))
            index += 1
            continue
        else:
            pass
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

        chelou_links = [link for link in src_loaded.links if link.get("kind") == "data" and not all(action in ["download", "other"] for action in link.get("actions"))]  # ############################################################

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
                "series",
            ]
            try:
                if int(environ.get("HARD_MODE")):
                    md_dst = src_migrator.import_into_other_metadata(
                        copymark_abstract=False,  # FALSE EN PROD
                        copymark_title=False,  # FALSE EN PROD
                        copymark_catalog=environ.get("ISOGEO_CATALOG_MIGRATED"),
                        destination_metadata_uuid=trg_uuid,
                        exclude_fields=li_exclude_fields,
                        exclude_catalogs=li_cat_to_exclude,
                        switch_service_layers=True
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
                else:
                    pass
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

    csv_result = Path("./scripts/lille/csv/migrated_{}.csv".format(datetime.now().timestamp()))
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
        logger.info("{} metadatas haven't been migrated. Launch the script again pointing to '{}' file".format(len(li_failed), csv_result))
        csv_failed = Path(r"./scripts/lille/csv/migrate_failed.csv")
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
