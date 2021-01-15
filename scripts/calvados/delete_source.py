# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to delete "Département du Calvados" source metadatas after migration
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
import csv

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import BackupManager, MetadataDeleter


# load .env file
load_dotenv("./env/calvados.env", override=True)

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
        print("\r")


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
        Path("./scripts/calvados/delete_source_md.log"), "a", 5000000, 1
    )
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    if int(environ.get("HARD_MODE")) and int(environ.get("BACKUP")) == 0:
        is_ok = input("Are you sure you want to delete metadatas without backup? (y/n)")
        if is_ok == "y":
            print("OK let's go !")
        else:
            print("Cancellation of processing")
            exit()
    else:
        pass

    # Shortcuts
    wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")
    migrated_cat_uuid = environ.get("ISOGEO_CATALOG_MIGRATED")

    # ################# BUILD THE LIST OF SRC MD'S UUID TO DELETE #######################
    input_csv = Path(r"./scripts/calvados/csv/correspondances.csv")
    logger.info("Retrieving metadatas to delete UUID from {} csv file".format(input_csv))
    # retrieving md to delete uuid from csv file
    li_migrated = []
    # prepare csv reading
    fieldnames = [
        "source_uuid",
        "source_title",
        "source_name",
        "target_name",
        "target_uuid",
        "match_type"
    ]
    with input_csv.open() as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";", fieldnames=fieldnames)

        for row in reader:
            src_uuid = row.get("source_uuid")
            src_title = row.get("source_title")
            src_name = row.get("source_name")
            trg_name = row.get("target_name")
            trg_uuid = row.get("target_uuid")
            match_type = row.get("match_type")

            if reader.line_num > 1 and match_type == "perfect":
                li_migrated.append(
                    (src_uuid, trg_uuid)
                )
            else:
                pass

    # API client instanciation
    logger.info("Authenticating to Isogeo API")
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
    auth_timer = default_timer()

    trg_migrated_search = isogeo.search(
        group=wg_uuid,
        whole_results=True,
        query="catalog:{}".format(migrated_cat_uuid)
    )
    li_trg_migrated_uuid = [md.get("_id") for md in trg_migrated_search.results]

    # Build the list of metadata to delete UUID
    li_dlt_uuid = []
    # but first, check if each source md has actually been migrated
    for migrated in li_migrated:
        if migrated[1] in li_trg_migrated_uuid:
            li_dlt_uuid.append(migrated[0])
        else:
            pass

    # ################# BACKUP MDs THAT ARE GONNA BE DELETED #######################
    # instanciate backup manager
    if environ.get("BACKUP") == "1" and len(li_dlt_uuid) > 0:
        logger.info("======= {} listed metadatas gonna be backuped then deleted ========".format(len(li_dlt_uuid)))
        logger.info("-------------------------- BACKUP -------------------------------")
        # backup manager instanciation
        backup_path = Path(r"./scripts/calvados/_output/_backup")
        backup_mng = BackupManager(api_client=isogeo, output_folder=backup_path)
        # lauching backup
        amplitude = 50

        if len(li_dlt_uuid) > amplitude:
            bound_range = int(len(li_dlt_uuid) / amplitude)
            li_bound = []
            for i in range(bound_range + 1):
                li_bound.append(amplitude * i)
            li_bound.append(len(li_dlt_uuid))

            logger.info("Starting backup for {} rounds".format(len(li_bound) - 1))
            for i in range(len(li_bound) - 1):
                if default_timer() - auth_timer >= 6900:
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

                search_parameters = {"query": None, "specific_md": tuple(li_dlt_uuid[bound_inf:bound_sup])}
                try:
                    backup_mng.metadata(search_params=search_parameters)
                except Exception as e:
                    logger.info("an error occured : {}".format(e))
        else:
            search_parameters = {"query": None, "specific_md": tuple(li_dlt_uuid)}
            backup_mng.metadata(search_params=search_parameters)
    else:
        logger.info("============== {} listed metadatas gonna be deleted ===============".format(len(li_dlt_uuid)))
        logger.info("Backup was skipped")
        pass

    # ################# DELETE LISTED SRC MDs #######################
    logger.info("----------------------------- DELETION -----------------------------")

    start_time = default_timer()
    nb_deleted = 0
    nb_parsed = 0
    for uuid in li_dlt_uuid:
        nb_parsed += 1
        printProgressBar(
            iteration=nb_parsed,
            total=len(li_dlt_uuid),
            prefix='Processing progress:',
            length=100,
            suffix="- {} metadatas deleted".format(nb_parsed)
        )
        if default_timer() - auth_timer >= 6900:
            logger.debug("Manually refreshing token")
            backup_mng.isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass
        try:
            if int(environ.get("HARD_MODE")):
                isogeo.metadata.delete(uuid)
            else:
                pass
            nb_deleted += 1
        except Exception as e:
            logger.error(
                "Deletion of {} metadata failed".format(uuid)
            )
            logger.error(e)

    logger.info("{}/{} metadatas deleted in {}s.".format(nb_deleted, len(li_dlt_uuid), round(default_timer() - start_time, 2)))

    # md_dltr = MetadataDeleter(api_client=isogeo)
    # md_dltr.delete(
    #     metadata_ids_list=li_dlt_uuid,
    #     hard_mode=int(
    #         environ.get("HARD_MODE")
    #     )
    # )
    # print(len(md_dltr.deleted))
    # print(md_dltr.deleted)

    isogeo.close()

    logger.info("\n")