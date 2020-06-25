# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

"""
    Name:         Search and replace script for SMAVD metadata in 2020
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform search and replace.
                Code and logs are willingly verbose.

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
import csv

# 3rd party
import urllib3
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import SearchReplaceManager, BackupManager

# ##############################################################################
# ##### Stand alone program ########
# ##################################

# ------------ Log & debug ----------------
logger = logging.getLogger()
logging.captureWarnings(True)
logger.setLevel(logging.DEBUG)
# logger.setLevel(logging.INFO)

log_format = logging.Formatter(
    "%(asctime)s || %(levelname)s "
    "|| %(module)s - %(lineno)d ||"
    " %(funcName)s || %(message)s"
)

# debug to the file
log_file_handler = RotatingFileHandler(
    "{}.log".format(Path(__file__).stem), "a", 3000000, 1
)
log_file_handler.setLevel(logging.DEBUG)
log_file_handler.setFormatter(log_format)

# info to the console
log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.INFO)
log_console_handler.setFormatter(log_format)

logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)

# environment vars
load_dotenv("./env/smavd.env", override=True)

# ignore warnings related to the QA self-signed cert
if environ.get("ISOGEO_PLATFORM").lower() == "qa":
    urllib3.disable_warnings()


# chronometer
START_TIME = default_timer()

# establish isogeo connection
isogeo = Isogeo(
    client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
    client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
    auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
    platform=environ.get("ISOGEO_PLATFORM", "qa"),
    auth_mode="user_legacy",
)

# getting a token
isogeo.connect(
    username=environ.get("ISOGEO_USER_NAME"),
    password=environ.get("ISOGEO_USER_PASSWORD"),
)

# TIMER
auth_timer = default_timer()
logger.info("Connection to Isogeo established in {:5.2f}s.".format(default_timer() - START_TIME))

# instanciate Search and Replace manager
# prepare search and replace
# replace_patterns = {
#     "abstract": ("http://geocatalogue.smavd.org/?muid=", "http://geocatalogue.smavd.org/les-donnees-isogeo/"),
# }
replace_patterns = {
    "abstract": (r"/?muid=/", "/les-donnees-isogeo/"),
}


searchrpl_mngr = SearchReplaceManager(
    api_client=isogeo,
    attributes_patterns=replace_patterns,
)

# TIMER
instance_timer = default_timer() - auth_timer
logger.info(
    "Search and Replace Manager instanciated at: {:5.2f}s.".format(instance_timer)
)

# prepare search parameters

test_sample = (
    "fb21410983194b9980dc84c608df9723",
)

search_parameters = {
    "group": environ.get("ISOGEO_ORIGIN_WORKGROUP"),  # ISOGEO_ORIGIN_WORKGROUP en prod
    "specific_md": test_sample,
}

# launch search and replace in SAFE MODE to retrieve md list to backup
results = searchrpl_mngr.search_replace(search_params=search_parameters, safe=1, spec_car=True)
# retrieve the list of md to backup uuids
li_to_backup = [md._id for md in results]
# ------------------------------------ BACKUP --------------------------------------
if environ.get("BACKUP") == "1":
    logger.info("---------------------------- BACKUP ---------------------------------")
    # backup manager instanciation
    backup_path = Path(r"./scripts/smavd/search_and_replace/_output/_backup")
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

# example, save it to a CSV
output_csv = Path("./scripts/smavd/search_and_replace/_output/{}.csv".format(Path(__file__).stem))
csv.register_dialect(
    "pipe", delimiter="|", quoting=csv.QUOTE_ALL, lineterminator="\r\n"
)  # create dialect

with output_csv.open("w", newline="", encoding="utf8") as csvfile:
    # csv config
    results_writer = csv.writer(csvfile, dialect="pipe")
    # headers
    results_writer.writerow(["metadata_uuid", *replace_patterns])
    # parse results
    for replaced in results:
        # remove line returns to avoid issues in CSV formatting
        if replaced.abstract and "\n" in replaced.abstract:
            replaced.abstract = replaced.abstract.replace("\n", " ")
        # write rows
        results_writer.writerow(
            [getattr(replaced, i) for i in ["_id", *replace_patterns]]
        )

# Launch search and replace for real
# searchrpl_mngr.search_replace(search_params=search_parameters, safe=0)
# print(len(results))

isogeo.close()
