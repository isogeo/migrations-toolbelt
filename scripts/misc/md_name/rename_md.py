# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to add a rename some IGN-F metadatas.
    Author:       Isogeo
    Purpose:      Script using isogeo-pysdk to update events.

    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
from os import environ
from pathlib import Path
from timeit import default_timer
from datetime import datetime

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, Metadata

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator

# load .env file
load_dotenv("./env/.env", override=True)

# #############################################################################
# ########## Main program ###############
# #######################################

if __name__ == "__main__":

    # Shortcuts
    dict_md_to_parse = {
        "b077525b7e16457d9b7a942e93b653ce": (
            "ARRONDISSEMENT", "ARRONDISSEMENT_DEPARTEMENTAL"
        ),
        "f19d73d0e8524472a9f308112d4c1bd1": (
            "ZONE_HABITAT", "ZONE_HABITAT_MAIRIE"
        )
    }

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

    li_parsed = []
    for uuid, infos in dict_md_to_parse.items():

        if default_timer() - auth_timer >= 6900:
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass

        migrator = MetadataDuplicator(
            api_client=isogeo, source_metadata_uuid=uuid
        )

        if migrator.metadata_source.name == infos[0]:
            migrator.metadata_source.name = infos[1]
            print("{} metadata's name gonna be changed from {} to {}".format(uuid, infos[0], infos[1]))
        else:
            print("{} metadata's current isn't {} as expected but {}".format(uuid, infos[0], migrator.metadata_source.name))
            continue

        duplicated = migrator.duplicate_into_same_group(
            copymark_abstract=False,  # FALSE EN PROD
            copymark_title=False,  # FALSE EN PROD
            copymark_catalog="5c743581c7724f69bdab6b542d003b7a",
            switch_service_layers=True
        )

    isogeo.close()
