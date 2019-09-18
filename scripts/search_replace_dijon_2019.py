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
    ".log".format(Path(__file__).name), "a", 3000000, 1
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
load_dotenv("dev.env", override=True)

# ignore warnings related to the QA self-signed cert
if environ.get("ISOGEO_PLATFORM").lower() == "qa":
    urllib3.disable_warnings()

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

# instanciate Search and Replace manager
# prepare search and replace
replace_patterns = {
    "title": ("Grand Dijon", "Dijon Métropole"),
    "abstract": ("Grand Dijon", "Dijon Métropole"),
}

searchrpl_mngr = SearchReplaceManager(
    api_client=isogeo,
    output_folder="./_output/search_replace/",
    attributes_patterns=replace_patterns,
)

# prepare search parameters
search_parameters = {"group": "542bc1e743f6464fb471dc48f0da02d2"}

# launch search and replace
searchrpl_mngr.search_replace(search_params=search_parameters, safe=1)

# close connection
isogeo.close()
