# -*- coding: UTF-8 -*-
#! python3
# ##############################################################################
# ########## Libraries #############

# Standard Library
from os import environ

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load dijon.env file
load_dotenv("env/.env", override=True)

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

src_md_uuid = "34864ea23b714d3fb7aaa8131a76825d"
trg_md_uuid = "596ae6730d76458395c74052240af984"

migrator = MetadataDuplicator(
    api_client=isogeo,
    source_metadata_uuid=src_md_uuid
)

migrator.import_into_other_metadata(
    destination_metadata_uuid=trg_md_uuid,
)

isogeo.close()