# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

"""
    Name:         Search and replace script for IGN logo link
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
logger.setLevel(logging.INFO)

log_format = logging.Formatter(
    "%(asctime)s || %(levelname)s "
    "|| %(module)s - %(lineno)d ||"
    " %(funcName)s || %(message)s"
)

# debug to the file
log_file_handler = RotatingFileHandler(
    "{}.log".format(Path(__file__).stem), "a", 3000000, 1
)
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_format)

# info to the console
log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.INFO)
log_console_handler.setFormatter(log_format)

logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)

# environment vars
load_dotenv("./env/misc.env", override=True)

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
# prepare replace patterns
replace_patterns = {
    "abstract": (
        "!\[Logo\ de\ l'IGN\]\(https://www\.ensg\.eu/\-MEP0\-/apv/logo_IGN\.png\)",
        "![Logo de l'IGN](https://www.ensg.eu/-MEP0-/apv/logo_IGN.png)"
    )
}

searchrpl_mngr = SearchReplaceManager(
    api_client=isogeo,
    attributes_patterns=replace_patterns,
)

li_wg_uuid = environ.get("IGN_LOGO_INVOLVED_WG").split(";")  # PROD
li_wg = [isogeo.workgroup.get(wg_uuid) for wg_uuid in li_wg_uuid]
logger.info("{} workgroups gonna be inspected\n".format(len(li_wg_uuid)))

global_results = []

for wg in li_wg:
    # prepare search parameters
    search_parameters = {
        "group": wg._id
    }
    # launch search and replace in SAFE MODE to retrieve md list to backup
    if default_timer() - auth_timer >= 6900:
        logger.info("Manually refreshing token")
        isogeo.connect(
            username=environ.get("ISOGEO_USER_NAME"),
            password=environ.get("ISOGEO_USER_PASSWORD"),
        )
        auth_timer = default_timer()
    else:
        pass
    wg_results = searchrpl_mngr.search_replace(search_params=search_parameters, safe=1)
    for md in wg_results:
        global_results.append(md)
    logger.info("--> {} metadata of {} workgroup match the pattern".format(len(wg_results), wg.contact.get("name")))

logger.info("==> {} metadata match the pattern into {} inspected workgroups".format(len(global_results), len(li_wg_uuid)))
# retrieve the list of md to backup uuids
li_to_backup = [md._id for md in global_results]
# ------------------------------------ BACKUP --------------------------------------
if environ.get("BACKUP") == "1" and len(li_to_backup):
    logger.info("---------------------------- BACKUP ---------------------------------")
    # backup manager instanciation
    backup_path = Path(r"./scripts/misc/search_replace/_output/_backup")
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

# Launch search and replace for real
for wg in li_wg:
    # prepare search parameters
    search_parameters = {
        "group": wg._id
    }
    # launch search and replace in SAFE MODE to retrieve md list to backup
    if default_timer() - auth_timer >= 6900:
        logger.info("Manually refreshing token")
        isogeo.connect(
            username=environ.get("ISOGEO_USER_NAME"),
            password=environ.get("ISOGEO_USER_PASSWORD"),
        )
        auth_timer = default_timer()
    else:
        pass
    searchrpl_mngr.search_replace(search_params=search_parameters, safe=0)

isogeo.close()

# example, save it to a CSV
output_csv = Path("./scripts/misc/search_replace/csv/{}.csv".format(Path(__file__).stem))
csv.register_dialect(
    "pipe", delimiter=";", quoting=csv.QUOTE_ALL, lineterminator="\r\n"
)  # create dialect

with output_csv.open("w", newline="", encoding="utf8") as csvfile:
    # csv config
    results_writer = csv.writer(csvfile, dialect="pipe")
    # headers
    results_writer.writerow(["workgroup_name", "metadata_uuid", *replace_patterns])
    # parse results
    for replaced in global_results:
        # remove line returns to avoid issues in CSV formatting
        if replaced.abstract and "\n" in replaced.abstract:
            replaced.abstract = replaced.abstract.replace("\n", " ")
        li_line = [replaced._creator.get("contact").get("name")]
        for i in ["_id", *replace_patterns]:
            li_line.append(getattr(replaced, i))
        # write rows
        results_writer.writerow(
            li_line
        )
