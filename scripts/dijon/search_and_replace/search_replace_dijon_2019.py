# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script for magOSM data in 2019
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import logging
import csv
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer

# 3rd party
import urllib3
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import SearchReplaceManager

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
# load_dotenv("dev.env", override=True)
# load_dotenv("prod.env", override=True)
load_dotenv("dijon.env", override=True)

# ignore warnings related to the QA self-signed cert
if environ.get("ISOGEO_PLATFORM").lower() == "qa":
    urllib3.disable_warnings()


# chronometer
START_TIME = default_timer()

# establish isogeo connection
isogeo = Isogeo(
    client_id=environ.get("ISOGEO_API_USER_CLIENT_ID"),
    client_secret=environ.get("ISOGEO_API_USER_CLIENT_SECRET"),
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
auth_timer = default_timer() - START_TIME
logger.info("Connection to Isogeo established in {:5.2f}s.".format(auth_timer))

# instanciate Search and Replace manager
# prepare search and replace
replace_patterns = {
    "title": ("Grand Dijon", "Dijon Métropole"),
    "abstract": ("Grand Dijon", "Dijon Métropole"),
}

dict_prepositions = {
    "la Communauté Urbaine du ": "",
    "au ": "à ",
    "du ": "de ",
    "le ": "",
}

searchrpl_mngr = SearchReplaceManager(
    api_client=isogeo,
    attributes_patterns=replace_patterns,
    prepositions=dict_prepositions,
)

# TIMER
instance_timer = default_timer() - auth_timer
logger.info(
    "Search and Replace Manager instanciated at: {:5.2f}s.".format(instance_timer)
)


# prepare search parameters

test_sample = (
    "ecf84a3d3f3949a38172c3d013dc02e4",
    "02e662b4e3f84f88953e4a8dccd0e8fa",
    "b0b2de70c6724a9cb849f597bf262cf7",
    "f16c036567bd4355b350df29779f06ce",
    "c65829c8723245a7b70ed98f08397c9d",
)

search_parameters = {
    "group": environ.get("ISOGEO_MIGRATION_WORKGROUP"),  # ISOGEO_ORIGIN_WORKGROUP en prod
    # "specific_md": test_sample,
}

# # launch search and replace in SAFE MODE
# results = searchrpl_mngr.search_replace(search_params=search_parameters, safe=1)

# # example, save it to a CSV
# output_csv = Path("./scripts/dijon/search_and_replace/_output/{}.csv".format(Path(__file__).stem))
# csv.register_dialect(
#     "pipe", delimiter="|", quoting=csv.QUOTE_ALL, lineterminator="\r\n"
# )  # create dialect

# with output_csv.open("w", newline="", encoding="utf8") as csvfile:
#     # csv config
#     results_writer = csv.writer(csvfile, dialect="pipe")
#     # headers
#     results_writer.writerow(["metadata_uuid", *replace_patterns])
#     # parse results
#     for replaced in results:
#         # remove line returns to avoid issues in CSV formatting
#         if replaced.abstract and "\n" in replaced.abstract:
#             replaced.abstract = replaced.abstract.replace("\n", " ")
#         # write rows
#         results_writer.writerow(
#             [getattr(replaced, i) for i in ["_id", *replace_patterns]]
#         )

# TIMER
auth_timer = default_timer() - START_TIME
logger.info("Connection to Isogeo established in {:5.2f}s.".format(auth_timer))

searchrpl_mngr.search_replace(search_params=search_parameters, safe=0)
# print(len(results))
