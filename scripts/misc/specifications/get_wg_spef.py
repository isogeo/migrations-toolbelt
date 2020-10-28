# -*- coding: UTF-8 -*-
#! python3

# ##############################################################################
# ########## Libraries #############

# Standard Library
from os import environ
from pathlib import Path
from pprint import pprint

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo


# load .env file
load_dotenv("./env/.env", override=True)

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
wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP")

wg_spec = isogeo.specification.listing(
    workgroup_id=wg_uuid,
    include="all"
)

pprint(wg_spec)

isogeo.close()