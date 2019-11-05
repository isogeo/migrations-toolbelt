# -*- coding: UTF-8 -*-
#! python3  # noqa E265

"""Usage from the repo root folder:

    .. code-block:: python

        # for whole test
        python -m unittest tests.test_backup_metadata
        # for specific python -m unittest
        python -m unittest tests.test_backup_metadata.TestBackupMetadata.test_backup_metadata_json

"""

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import logging
import unittest
import urllib3
from os import environ
from pathlib import Path
from socket import gethostname
from sys import _getframe, exit
from time import gmtime, sleep, strftime

# 3rd party
from dotenv import load_dotenv
from isogeo_pysdk import Isogeo

# module target
from isogeo_migrations_toolbelt import BackupManager


# #############################################################################
# ######## Globals #################
# ##################################

if Path("dev.env").exists():
    load_dotenv("dev.env", override=True)

# host machine name - used as discriminator
hostname = gethostname()

# #############################################################################
# ########## Helpers ###############
# ##################################


def get_test_marker():
    """Returns the function name."""
    return "TEST_MigrationsToolbelt - {}".format(_getframe(1).f_code.co_name)


# #############################################################################
# ########## Classes ###############
# ##################################


class TestBackupMetadata(unittest.TestCase):
    """Test metadata beckup."""

    # -- Standard methods --------------------------------------------------------
    @classmethod
    def setUpClass(cls):
        """Executed when module is loaded before any test."""
        # checks
        if not environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID") or not environ.get(
            "ISOGEO_API_USER_LEGACY_CLIENT_SECRET"
        ):
            logging.critical("No API credentials set as env variables.")
            exit()
        else:
            pass

        # ignore warnings related to the QA self-signed cert
        if environ.get("ISOGEO_PLATFORM").lower() == "qa":
            urllib3.disable_warnings()

        # API connection
        cls.isogeo = Isogeo(
            auth_mode="user_legacy",
            client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
            client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
            auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
            platform=environ.get("ISOGEO_PLATFORM", "qa"),
        )
        # getting a token
        cls.isogeo.connect(
            username=environ.get("ISOGEO_USER_NAME"),
            password=environ.get("ISOGEO_USER_PASSWORD"),
        )

        # fixture metadata
        cls.fixture_metadata = cls.isogeo.metadata.get(
            metadata_id=environ.get("ISOGEO_METADATA_FIXTURE_UUID"), include="all"
        )

    def setUp(self):
        """Executed before each test."""
        # tests stuff
        self.discriminator = "{}_{}".format(
            hostname, strftime("%Y-%m-%d_%H%M%S", gmtime())
        )

    def tearDown(self):
        """Executed after each test."""
        sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Executed after the last test."""
        # close sessions
        cls.isogeo.close()

    # -- TESTS ---------------------------------------------------------
    def test_backup_metadata_json(self):
        """duplicate_into_other_group"""
        # load source
        backup_mngr = BackupManager(api_client=self.isogeo, output_folder="./output")

        # build search parameters. For example to filter on two specifics metadata
        search_parameters = {
            "query": None,
            "specific_md": (self.fixture_metadata._id,),
        }

        # launch the backup
        backup_mngr.metadata(search_params=search_parameters)
