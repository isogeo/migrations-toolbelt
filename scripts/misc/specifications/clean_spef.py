# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Migration script to Migrate Route500 for Rouen
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
from time import sleep
from pprint import pprint
from time import sleep

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import Isogeo, IsogeoChecker

# submodules
from isogeo_migrations_toolbelt import MetadataDuplicator, BackupManager

# load dijon.env file
load_dotenv("env/rouen.env", override=True)

checker = IsogeoChecker()

if __name__ == "__main__":

    # ############################### MIGRATING ###############################
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

    li_md_uuid = ["e945938201194a9b9b0f352d05fb8d34", "883ace72c8a246dea62349924fd0425a"]
    li_spef_prefix = ["IGN - Descriptif de contenu de ROUTE 500", "spec_test"]
    wg_uuid = "0929bd0968bc4e19a6b58f65bdb4dda8"

    li_spef = isogeo.specification.listing(
        workgroup_id=wg_uuid,
        include="all",
        caching=0
    )

    for uuid in li_md_uuid:
        md = isogeo.metadata.get(
            metadata_id=uuid,
            include="all"
        )
        for spef in md.specifications:
            if any(spef.get("specification").get("name").startswith(spef_prefix) for spef_prefix in li_spef_prefix):
                isogeo.specification.dissociate_metadata(
                    metadata=md,
                    specification_id=spef.get("specification").get("_id")
                )

    for spef in li_spef:
        if any(spef.get("name").startswith(spef_prefix) for spef_prefix in li_spef_prefix):
            isogeo.specification.delete(
                workgroup_id=wg_uuid,
                specification_id=spef.get("_id")
            )

    # #####################################################################################################

    # from isogeo_pysdk import Specification

    # li_spef = isogeo.specification.listing(
    #     workgroup_id="0929bd0968bc4e19a6b58f65bdb4dda8",
    #     include="all"
    # )

    # pprint(len(li_spef))

    # new_specification = Specification()
    # new_specification.link = "http://test"
    # new_specification.name = "spec_test"
    # new_specification.published = "1996-05-17T00:00:00"
    # specification = isogeo.specification.create(
    #     workgroup_id="0929bd0968bc4e19a6b58f65bdb4dda8",
    #     specification=new_specification,
    #     check_exists=0
    # )

    # li_spef = isogeo.specification.listing(
    #     workgroup_id="0929bd0968bc4e19a6b58f65bdb4dda8",
    #     include="all",
    #     caching=0
    # )

    # pprint(len(li_spef))

    isogeo.close()
