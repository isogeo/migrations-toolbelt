# -*- coding: UTF-8 -*-
#! python3  # noqa E265

"""Usage from the repo root folder:

    .. code-block:: python

        # for whole test
        python -m unittest tests.test_duplicate_import_to_other_metadata
        # for specific python -m unittest
        python -m unittest tests.test_duplicate_import_to_other_metadata.TestDuplicationImportMetadata.test_duplicate_with_copymark

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
from isogeo_pysdk import Isogeo, Metadata

# module target
from isogeo_migrations_toolbelt import MetadataDuplicator


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


class TestDuplicationImportMetadata(unittest.TestCase):
    """Test metadata duplicator."""

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

        # fixture metadata - source
        cls.fixture_metadata = cls.isogeo.metadata.get(
            metadata_id=environ.get("ISOGEO_METADATA_FIXTURE_UUID"), include="all"
        )

    def setUp(self):
        """Executed before each test."""
        # tests stuff
        self.discriminator = "{}_{}".format(
            hostname, strftime("%Y-%m-%d_%H%M%S", gmtime())
        )

        # target
        # fixture metadata - destination
        md_target = Metadata(
            format="shp",
            name="network_analisis.shp",
            path="//datagis/path_to_vectors/network_analisis.shp",
            title="{} - {}".format(
                get_test_marker(),
                "{}_{}".format(hostname, strftime("%Y-%m-%d_%H%M%S", gmtime())),
            ),
            type="vectorDataset",
        )
        # create it online
        self.fixture_metadata_target = self.isogeo.metadata.create(
            workgroup_id=self.fixture_metadata.groupId,
            metadata=md_target,
            return_basic_or_complete=True,
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
    def test_duplicate_without_copymark(self):
        """duplicate_into_other_group"""
        # load source
        md_duplicator = MetadataDuplicator(
            api_client=self.isogeo,
            source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID"),
        )

        # duplicate it
        new_md = md_duplicator.import_into_other_metadata(
            destination_metadata_uuid=self.fixture_metadata_target._id,
            copymark_title=False,
            copymark_abstract=False,
        )

        # compare results
        self.assertEqual(self.fixture_metadata.title, new_md.title)

        # delete created metadata
        self.isogeo.metadata.delete(new_md._id)

    def test_duplicate_with_copymark(self):
        """duplicate_into_other_group"""
        # load source
        md_duplicator = MetadataDuplicator(
            api_client=self.isogeo,
            source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID"),
        )

        # duplicate it
        new_md = md_duplicator.import_into_other_metadata(
            destination_metadata_uuid=self.fixture_metadata_target._id,
            copymark_title=True,
            copymark_abstract=True,
        )

        # compare results
        self.assertNotEqual(self.fixture_metadata.abstract, new_md.abstract)
        self.assertEqual(self.fixture_metadata.title + " [COPIE]", new_md.title)
        self.assertNotEqual(self.fixture_metadata.title, new_md.title)

        # delete created metadata
        self.isogeo.metadata.delete(new_md._id)
