# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to try MetadataDeleter
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

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import BackupManager, MetadataDeleter


# load .env file
load_dotenv("./env/misc.env", override=True)

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

    logging.getLogger("isogeo_migrations_toolbelt").propagate = False

    log_format = logging.Formatter(
        "%(asctime)s || %(levelname)s "
        "|| %(module)s - %(lineno)d ||"
        " %(funcName)s || %(message)s"
    )

    # debug to the file
    log_file_handler = RotatingFileHandler(
        Path("./scripts/misc/delete_md/delete_except_cat_md.log"), "a", 5000000, 1
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

    wg_deletion_target = "4b729516af434ba0b2c816216440a6ad"
    cat_to_keep = "9be9634266ca4d49926321e0be482594"

    # ################# MAKE THE LIST OF SRC MD'S UUID TO DELETE #######################
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

    logger.info("Retrieving metadatas to delete from {} workgroup".format(wg_deletion_target))
    to_delete = isogeo.search(
        group=wg_deletion_target,
        whole_results=True
    )

    to_keep_tag = "catalog:{}".format(cat_to_keep)

    # listing
    li_dlt_uuid = [md.get("_id") for md in to_delete.results if to_keep_tag not in md.get("tags")]

    # ################# BACKUP MDs THAT ARE GONNA BE DELETED #######################
    # instanciate backup manager
    if environ.get("BACKUP") == "1" and len(li_dlt_uuid) > 0:
        logger.info("========= {} source metadatas listed gonna be backuped then deleted =========".format(len(li_dlt_uuid)))
        logger.info("-------------------------- BACKUP -------------------------------")
        # backup manager instanciation
        backup_path = Path(r"./scripts/misc/delete_md/_output/_backup")
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
        logger.info("=========== {} source metadatas listed gonna be deleted ===========".format(len(li_dlt_uuid)))
        logger.info("Backup was skipped")
        pass

    # ################# DELETE LISTED SRC MDs #######################
    logger.info("-------------------------------- DELETION --------------------------------")

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
