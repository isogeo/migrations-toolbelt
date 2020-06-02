# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to associate service layers to metadatas according to a csv file 
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
from os import environ
from pathlib import Path
# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/rouen.env", override=True)

if __name__ == "__main__":

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

    service_md_uuid = tuple(environ.get("NEW_SERVICE_MD_UUIDS").split(";"))
