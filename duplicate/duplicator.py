# -*- coding: UTF-8 -*-
#! python3

# ------------------------------------------------------------------------------
# Name:         Metadata Duplicator
# Purpose:      Generic module to perform metadata duplication
# Author:       Isogeo
#
# Python:       3.6+
# ------------------------------------------------------------------------------

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import logging
import urllib3
from datetime import datetime
from os import environ
from time import sleep

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import IsogeoSession
from isogeo_pysdk.models import Catalog, Contact, CoordinateSystem, Event, License, Limitation, Metadata, Specification
from isogeo_pysdk.checker import IsogeoChecker

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)

checker = IsogeoChecker()

# environment vars
load_dotenv(".env", override=True)
WG_TEST_UUID = environ.get("ISOGEO_WORKGROUP_TEST_UUID")

# ignore warnings related to the QA self-signed cert
if environ.get("ISOGEO_PLATFORM").lower() == "qa":
    urllib3.disable_warnings()

# ############################################################################
# ########## Classes #############
# ################################


class MetadataDuplicator(object):
    def __init__(self, api_client: IsogeoSession, source_metadata_uuid: str):
        """[summary] 
        """
        # store API client
        self.api_client = api_client

        # check metadatas UUID
        if not checker.check_is_uuid(source_metadata_uuid):
            raise ValueError(
                "Passed source metadata UUID is not a correct UUID: {}".format(
                    source_metadata_uuid
                )
            )
        else:
            pass

        # store the source metadata
        self.metadata_source = self.api_client.metadata.get(
            metadata_id=source_metadata_uuid, include="all"
        )

    # -- DUPLICATION MODES -----------------------------------------------------------------
    def duplicate_into_same_group(
        self,
        copymark_catalog: str = None,
        copymark_title: bool = True,
        copymark_abstract: bool = True,
    ) -> Metadata:
        """Create an exact copy of the metadata source in the same workgroup.
        It can apply some copy marks to distinguish the copy from the original.
        
        :param str copymark_catalog: add the new metadata to this additionnal catalog, defaults to None
        :param bool copymark_title: add a [COPY] mark at the end of the new metadata (default: {True}), defaults to True
        :param bool copymark_abstract: add a [Copied from](./source_uuid)] mark at the end of the new metadata abstract, defaults to True

        :return: the newly created Metadata
        :rtype: Metadata

        :Example:
        >>> # instanciate the metadata duplicator
        >>> md_source = MetadataDuplicator(
            api_client=isogeo,
            source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID")
            )
        >>> # duplicate it
        >>> new_md = md_source.duplicate_into_same_group()
        """
        # duplicate local metadata
        md_to_create = self.metadata_source

        # edit some fields according to the options
        if copymark_title:
            md_to_create.title += " [COPIE]"

        if copymark_abstract:
            md_to_create.abstract += "\n\n----\n\n > Cette métadonnée a été créée à partir de [cette autre métadonnée](/groups/{}/resources/{}).".format(
                self.metadata_source._creator.get("_id"), self.metadata_source._id
            )

        # create it online: it will create only the attributes which are at the base
        md_dest = self.api_client.metadata.create(
            workgroup_id=self.metadata_source._creator.get("_id"),
            metadata=md_to_create,
            check_exists=0,
        )
        logger.info(
        "Duplicate has been created: {} ({}). Let's import the associated resources and subresources.".format(
            md_dest.title, md_dest._id
        ))

        # let the API get a rest ;)
        sleep(0.5)

        # NOW PERFORM DUPLICATION OF SUBRESOURCES
        # Catalogs
        li_catalogs_uuids = [
            tag[8:] for tag in self.metadata_source.tags if tag.startswith("catalog:")
        ]
        if copymark_catalog is not None:
            li_catalogs_uuids.append(copymark_catalog)

        if len(li_catalogs_uuids):
            for cat_uuid in li_catalogs_uuids:
                # retrieve online catalog
                catalog = self.api_client.catalog.get(
                    workgroup_id=self.metadata_source._creator.get("_id"),
                    catalog_id=cat_uuid,  # CHANGE IT with SDK version >= 3.0.1
                )
                # associate the metadata with
                self.api_client.catalog.associate_metadata(
                    metadata=md_dest, catalog=catalog
                )
            logger.info("{} catalogs imported.".format(len(li_catalogs_uuids)))

        # Conditions / Licenses (CGUs)
        if len(self.metadata_source.conditions):
            for condition in self.metadata_source.conditions:
                licence = License(**condition.get("license"))
                description = condition.get("description")
                self.api_client.license.associate_metadata(
                    metadata=md_dest,
                    license=licence,
                    description=description,
                    force=1
                )
            logger.info(
                "{} conditions (license + specific description) have been imported.".format(
                    len(self.metadata_source.conditions)
                )
            )

        # Contacts
        if len(self.metadata_source.contacts):
            for ct in self.metadata_source.contacts:
                contact = Contact(**ct.get("contact"))
                self.api_client.contact.associate_metadata(
                    metadata=md_dest, contact=contact, role=ct.get("role")
                )
            logger.info(
                "{} contacts imported.".format(len(self.metadata_source.contacts))
            )

        # Coordinate-systems
        if isinstance(self.metadata_source.coordinateSystem, dict):
            srs = CoordinateSystem(**self.metadata_source.coordinateSystem)
            self.api_client.srs.associate_metadata(
                metadata=md_dest,
                coordinate_system=srs
            )
            logger.info(
                "Coordinate-system {} imported.".format(srs.code)
            )

        # Events
        if len(self.metadata_source.events):
            for evt in self.metadata_source.events:
                event = Event(**evt)
                event.date = event.date[:10]
                self.api_client.metadata.events.create(metadata=md_dest, event=event)
            logger.info(
                "{} events have been imported.".format(len(self.metadata_source.events))
            )

        # Feature attributes
        if self.metadata_source.type == "vectorDataset" and len(self.metadata_source.featureAttributes):
            self.api_client.metadata.attributes.import_from_dataset(
                metadata_source=self.metadata_source, metadata_dest=md_dest
            )
            logger.info(
                "{} feature attributes have been imported.".format(
                    len(self.metadata_source.featureAttributes)
                )
            )

        # Keywords (including INSPIRE themes)
        li_keywords = self.api_client.metadata.keywords(
            self.metadata_source, include=[]
        )
        if len(li_keywords):
            for kwd in li_keywords:
                # retrieve online keyword
                keyword = self.api_client.keyword.get(keyword_id=kwd.get("_id"), include=[])
                # associate the metadata with
                self.api_client.keyword.tagging(metadata=md_dest, keyword=keyword)
            logger.info("{} keywords imported.".format(len(li_keywords)))

        # Limitations (CGUs)
        if len(self.metadata_source.limitations):
            for lim in self.metadata_source.limitations:
                limitation = Limitation(**lim)
                self.api_client.metadata.limitations.create(
                    metadata=md_dest,
                    limitation=limitation
                )
            logger.info(
                "{} limitations have been imported.".format(
                    len(self.metadata_source.limitations)
                )
            )

        # Specifications
        if len(self.metadata_source.specifications):
            for spec in self.metadata_source.specifications:
                specification = Specification(**spec.get("specification"))
                isConformant = spec.get("conformant")
                self.api_client.specification.associate_metadata(
                    metadata=md_dest,
                    specification=specification,
                    conformity=isConformant,
                )
            logger.info(
                "{} specifications have been imported.".format(
                    len(self.metadata_source.specifications)
                )
            )

        return md_dest

    def duplicate_into_other_group(self, destination_workgroup_uuid: str) -> Metadata:
        """[summary]
        
        Returns:
            Metadata -- [description]
        """
        # check metadatas UUID
        if not checker.check_is_uuid(destination_workgroup_uuid):
            raise ValueError(
                "Destination workgroup UUID is not a correct UUID: {}".format(
                    destination_workgroup_uuid
                )
            )
        else:
            pass

        return True

    def import_into_other_metadata(self, destination_metadata_uuid: str) -> Metadata:
        """[summary]
        
        Returns:
            Metadata -- [description]
        """
        # check metadatas UUID
        if not checker.check_is_uuid(destination_metadata_uuid):
            raise ValueError(
                "Destination workgroup UUID is not a correct UUID: {}".format(
                    destination_metadata_uuid
                )
            )
        else:
            pass

        return True

    # -- DUPLICATION TOOLING -----------------------------------------------------------


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    from logging.handlers import RotatingFileHandler

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
    log_file_handler = RotatingFileHandler("dev_debug.log", "a", 3000000, 1)
    log_file_handler.setLevel(logging.DEBUG)
    log_file_handler.setFormatter(log_format)

    # info to the console
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.INFO)
    log_console_handler.setFormatter(log_format)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)
    # establish isogeo connection
    isogeo = IsogeoSession(
        client_id=environ.get("ISOGEO_API_USER_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_CLIENT_SECRET"),
        auto_refresh_url="{}/oauth/token".format(environ.get("ISOGEO_ID_URL")),
        platform=environ.get("ISOGEO_PLATFORM", "qa"),
    )

    # getting a token
    isogeo.connect(
        username=environ.get("ISOGEO_USER_NAME"),
        password=environ.get("ISOGEO_USER_PASSWORD"),
    )

    # prepare duplication
    md_source = MetadataDuplicator(
        api_client=isogeo,
        source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID"),
    )

    # print(dir(md_source))

    new_md = md_source.duplicate_into_same_group(
        copymark_catalog="88836154514a45e4b073cfaf350eea02"
    )
    print(new_md._id, new_md.title)

    # achievefinish connection
    isogeo.close()
