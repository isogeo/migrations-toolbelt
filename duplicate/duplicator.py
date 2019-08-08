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
from pprint import pprint
from time import sleep

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import IsogeoSession
from isogeo_pysdk.models import (
    Catalog,
    Contact,
    CoordinateSystem,
    Event,
    License,
    Limitation,
    Metadata,
    ServiceLayer,
    Specification,
    Workgroup,
)
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
        self.isogeo = api_client

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
        self.metadata_source = self.isogeo.metadata.get(
            metadata_id=source_metadata_uuid, include="all"
        )

    # -- DUPLICATION MODES -----------------------------------------------------------------
    def duplicate_into_same_group(
        self,
        copymark_catalog: str = None,
        copymark_title: bool = True,
        copymark_abstract: bool = True,
        switch_service_layers: bool = False,
    ) -> Metadata:
        """Create an exact copy of the metadata source in the same workgroup.
        It can apply some copy marks to distinguish the copy from the original.
        
        :param str copymark_catalog: add the new metadata to this additionnal catalog. Defaults to None
        :param bool copymark_title: add a [COPY] mark at the end of the new metadata (default: {True}). Defaults to True
        :param bool copymark_abstract: add a [Copied from](./source_uuid)] mark at the end of the new metadata abstract. Defaults to True
        :param bool switch_service_layers: a service layer can't be associated to many datasetes. \
            If this option is enabled, service layers are removed from the metadata source then added to the new one. Defaults to False

        :returns: the newly created Metadata
        :rtype: Metadata

        .. code-block:: python
        
            # instanciate the metadata duplicator
            md_source = MetadataDuplicator(
                isogeo=isogeo,
                source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID")
                )
            # duplicate it
            new_md = md_source.duplicate_into_same_group()

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
        if self.metadata_source.type == "service":
            # if it's a service, so use the helper
            md_dst = self.isogeo.services.create(
                workgroup_id=self.metadata_source._creator.get("_id"),
                service_url=self.metadata_source.path,
                service_format=self.metadata_source.format,
                service_title=md_to_create.title,
                check_exists=0,
                ignore_avaibility=1,
            )
        else:
            md_dst = self.isogeo.metadata.create(
                workgroup_id=self.metadata_source._creator.get("_id"),
                metadata=md_to_create,
                check_exists=0,
            )

        logger.info(
            "Duplicate has been created: {} ({}). Let's import the associated resources and subresources.".format(
                md_dst.title, md_dst._id
            )
        )

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
                catalog = self.isogeo.catalog.get(
                    workgroup_id=self.metadata_source._creator.get("_id"),
                    catalog_id=cat_uuid,  # CHANGE IT with SDK version >= 3.0.1
                )
                # associate the metadata with
                self.isogeo.catalog.associate_metadata(metadata=md_dst, catalog=catalog)
            logger.info("{} catalogs imported.".format(len(li_catalogs_uuids)))

        # Conditions / Licenses (CGUs)
        if len(self.metadata_source.conditions):
            for condition in self.metadata_source.conditions:
                licence = License(**condition.get("license"))
                description = condition.get("description")
                self.isogeo.license.associate_metadata(
                    metadata=md_dst, license=licence, description=description, force=1
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
                self.isogeo.contact.associate_metadata(
                    metadata=md_dst, contact=contact, role=ct.get("role")
                )
            logger.info(
                "{} contacts imported.".format(len(self.metadata_source.contacts))
            )

        # Coordinate-systems
        if isinstance(self.metadata_source.coordinateSystem, dict):
            srs = CoordinateSystem(**self.metadata_source.coordinateSystem)
            self.isogeo.srs.associate_metadata(metadata=md_dst, coordinate_system=srs)
            logger.info("Coordinate-system {} imported.".format(srs.code))

        # Events
        if len(self.metadata_source.events):
            for evt in self.metadata_source.events:
                event = Event(**evt)
                event.date = event.date[:10]
                self.isogeo.metadata.events.create(metadata=md_dst, event=event)
            logger.info(
                "{} events have been imported.".format(len(self.metadata_source.events))
            )

        # Feature attributes
        if self.metadata_source.type == "vectorDataset" and len(
            self.metadata_source.featureAttributes
        ):
            self.isogeo.metadata.attributes.import_from_dataset(
                metadata_source=self.metadata_source, metadata_dest=md_dst
            )
            logger.info(
                "{} feature attributes have been imported.".format(
                    len(self.metadata_source.featureAttributes)
                )
            )

        # Keywords (including INSPIRE themes)
        li_keywords = self.isogeo.metadata.keywords(self.metadata_source, include=[])
        if len(li_keywords):
            for kwd in li_keywords:
                # retrieve online keyword
                keyword = self.isogeo.keyword.get(keyword_id=kwd.get("_id"), include=[])
                # associate the metadata with
                self.isogeo.keyword.tagging(
                    metadata=md_dst, keyword=keyword, check_exists=1
                )
            logger.info("{} keywords imported.".format(len(li_keywords)))

        # Limitations (CGUs)
        if len(self.metadata_source.limitations):
            for lim in self.metadata_source.limitations:
                limitation = Limitation(**lim)
                self.isogeo.metadata.limitations.create(
                    metadata=md_dst, limitation=limitation
                )
            logger.info(
                "{} limitations have been imported.".format(
                    len(self.metadata_source.limitations)
                )
            )

        # Service layers associated
        if self.metadata_source.type in ("rasterDataset", "vectorDataset") and len(
            self.metadata_source.serviceLayers
        ):
            if switch_service_layers:
                for service_layer in self.metadata_source.serviceLayers:
                    # remove the layer from the source
                    self.isogeo.metadata.layers.dissociate_metadata(
                        service=Metadata(
                            _id=service_layer.get("service").get("_id"), type="service"
                        ),
                        layer=ServiceLayer(_id=service_layer.get("_id")),
                        dataset=self.metadata_source,
                    )

                    # add the layer to the copy
                    self.isogeo.metadata.layers.associate_metadata(
                        service=Metadata(
                            _id=service_layer.get("service").get("_id"), type="service"
                        ),
                        layer=ServiceLayer(_id=service_layer.get("_id")),
                        dataset=md_dst,
                    )

                logger.info(
                    "{} service layers have been imported after they have been removed from the source.".format(
                        len(self.metadata_source.serviceLayers)
                    )
                )
            else:
                logger.info(
                    "{} service layers have NOT been imported because they stay associated with the source.".format(
                        len(self.metadata_source.serviceLayers)
                    )
                )

        # Specifications
        if len(self.metadata_source.specifications):
            for spec in self.metadata_source.specifications:
                specification = Specification(**spec.get("specification"))
                isConformant = spec.get("conformant")
                self.isogeo.specification.associate_metadata(
                    metadata=md_dst,
                    specification=specification,
                    conformity=isConformant,
                )
            logger.info(
                "{} specifications have been imported.".format(
                    len(self.metadata_source.specifications)
                )
            )

        return md_dst

    def duplicate_into_other_group(
        self,
        destination_workgroup_uuid: str,
        copymark_title: bool = True,
        copymark_abstract: bool = True,
    ) -> Metadata:
        """Create an exact copy of the metadata source into another workgroup.
        It can apply some copy marks to distinguish the copy from the original.
        
        :param str copymark_catalog: add the new metadata to this additionnal catalog. Defaults to None
        :param bool copymark_title: add a [COPY] mark at the end of the new metadata (default: {True}). Defaults to True
        :param bool copymark_abstract: add a [Copied from](./source_uuid)] mark at the end of the new metadata abstract. Defaults to True
        :param bool switch_service_layers: a service layer can't be associated to many datasetes. \
            If this option is enabled, service layers are removed from the metadata source then added to the new one. Defaults to False

        :returns: the newly created Metadata
        :rtype: Metadata

        :Example:

        .. code-block:: python

            # instanciate the metadata duplicator
            md_source = MetadataDuplicator(
                isogeo=isogeo,
                source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID")
                )
            # duplicate it
            new_md = md_source.duplicate_into_same_group()

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

        # duplicate local metadata
        md_to_create = self.metadata_source

        # edit some fields according to the options
        if copymark_title:
            md_to_create.title += " [COPIE]"

        if copymark_abstract:
            copymark_abstract_txt = "Cette métadonnée a été créée à partir de [cette autre métadonnée](/groups/{}/resources/{}).".format(
                self.metadata_source._creator.get("_id"), self.metadata_source._id
            )
            md_to_create.abstract = "{}\n\n----\n\n > {}".format(
                md_to_create.abstract, copymark_abstract_txt
            )

        # create it online: it will create only the attributes which are at the base
        if self.metadata_source.type == "service":
            # if it's a service, so use the helper
            md_dst = self.isogeo.services.create(
                workgroup_id=destination_workgroup_uuid,
                service_url=self.metadata_source.path,
                service_format=self.metadata_source.format,
                service_title=md_to_create.title,
                check_exists=0,
                ignore_avaibility=1,
            )
        else:
            md_dst = self.isogeo.metadata.create(
                workgroup_id=destination_workgroup_uuid,
                metadata=md_to_create,
                check_exists=0,
            )

        logger.info(
            "Duplicate has been created: {} ({}). Let's import the associated resources and subresources.".format(
                md_dst.title, md_dst._id
            )
        )

        # let the API get a rest ;)
        sleep(0.5)

        # NOW PERFORM DUPLICATION OF SUBRESOURCES

        # Catalogs

        # list and cache catalogs in the destination workgroup
        # parse source metadata catalogs
        li_catalogs_uuids = {
            tag[8:] for tag in self.metadata_source.tags if tag.startswith("catalog:")
        }

        if len(li_catalogs_uuids):
            # retrieve catalogs fo the destination group to match with source
            li_catalogs_grp_new = self.isogeo.catalog.listing(
                workgroup_id=destination_workgroup_uuid, include=[], caching=1
            )

            for cat_uuid in li_catalogs_uuids:
                # retrieve online catalog
                src_catalog = self.isogeo.catalog.get(
                    workgroup_id=self.metadata_source._creator.get("_id"),
                    catalog_id=cat_uuid,
                )

                # compare catalog name with destination group catalogs
                if src_catalog.name in self.isogeo._wg_catalogs_names:
                    dest_catalog = Catalog(
                        _id=self.isogeo._wg_catalogs_names.get(src_catalog.name)
                    )
                    logger.info(
                        "A catalog with the name '{}' already exists in the destination group. It'll be used.".format(
                            src_catalog.name
                        )
                    )
                else:
                    # create it on the new group
                    dest_catalog = self.isogeo.catalog.create(
                        workgroup_id=destination_workgroup_uuid,
                        catalog=src_catalog,
                        check_exists=1,
                    )
                    logger.info(
                        "Catalog '{}' has been created in the destination group. It'll be used.".format(
                            src_catalog.name
                        )
                    )

                # associate the metadata with the catalog of the destination group
                self.isogeo.catalog.associate_metadata(
                    metadata=md_dst, catalog=dest_catalog
                )
            logger.info("{} catalogs imported.".format(len(li_catalogs_uuids)))

        # Contacts

        # list and cache contacts in the destination workgroup
        li_contacts_grp_new = self.isogeo.contact.listing(
            workgroup_id=destination_workgroup_uuid, include=[], caching=1
        )

        if len(self.metadata_source.contacts):
            # list and cache contacts in the destination workgroup
            li_contacts_grp_new = self.isogeo.contact.listing(
                workgroup_id=destination_workgroup_uuid, include=[], caching=1
            )

            for ct in self.metadata_source.contacts:
                src_contact = Contact(**ct.get("contact"))
                if src_contact.type == "custom":
                    logger.info(
                        "Custom contact spotted: {} ({})".format(
                            src_contact.name, src_contact.email
                        )
                    )
                    # compare contact email with destination group contacts
                    if src_contact.email in self.isogeo._wg_contacts_emails:
                        dest_contact = Contact(
                            _id=self.isogeo._wg_contacts_emails.get(src_contact.email)
                        )
                        logger.info(
                            "A contact ({}) with the email ({}) already exists in the destination group (shared group or address-book). It'll be used.".format(
                                src_contact._id, src_contact.email
                            )
                        )
                    else:
                        # create it on the new group
                        dest_contact = self.isogeo.contact.create(
                            workgroup_id=destination_workgroup_uuid,
                            contact=src_contact,
                            check_exists=1,
                        )
                        logger.info(
                            "Contact '{}' has been created in the destination group. It'll be used.".format(
                                src_contact.name
                            )
                        )
                else:
                    logger.info(
                        "Contact group identified: {}. No need to enlarge the address-book.".format(
                            src_contact.name
                        )
                    )
                    dest_contact = src_contact

                # associate the contact with the metadata
                self.isogeo.contact.associate_metadata(
                    metadata=md_dst, contact=dest_contact, role=ct.get("role")
                )
            logger.info(
                "{} contacts imported.".format(len(self.metadata_source.contacts))
            )

        # Coordinate-systems
        if isinstance(self.metadata_source.coordinateSystem, dict):
            srs = CoordinateSystem(**self.metadata_source.coordinateSystem)
            # first check if the SRS is already available in the destination group
            group_srs = self.isogeo.srs.listing(workgroup_id=destination_workgroup_uuid)
            group_srs = [coordsys.get("code") for coordsys in group_srs]

            # if it's not present, so add it
            if srs.code not in group_srs:
                isogeo.srs.associate_workgroup(
                    workgroup=Workgroup(_id=destination_workgroup_uuid),
                    coordinate_system=srs,
                )
                logger.info(
                    "Coordinate-system {} was not associated with the destination workgroup. It's now done.".format(
                        srs.code
                    )
                )

            # associate SRS to the metadata
            self.isogeo.srs.associate_metadata(metadata=md_dst, coordinate_system=srs)
            logger.info("Coordinate-system {} imported.".format(srs.code))

        # Events
        if len(self.metadata_source.events):
            for evt in self.metadata_source.events:
                event = Event(**evt)
                event.date = event.date[:10]
                self.isogeo.metadata.events.create(metadata=md_dst, event=event)
            logger.info(
                "{} events have been imported.".format(len(self.metadata_source.events))
            )

        # Feature attributes
        if self.metadata_source.type == "vectorDataset" and len(
            self.metadata_source.featureAttributes
        ):
            self.isogeo.metadata.attributes.import_from_dataset(
                metadata_source=self.metadata_source, metadata_dest=md_dst
            )
            logger.info(
                "{} feature attributes have been imported.".format(
                    len(self.metadata_source.featureAttributes)
                )
            )

        # Keywords (including INSPIRE themes)
        li_keywords = self.isogeo.metadata.keywords(self.metadata_source, include=[])
        if len(li_keywords):
            for kwd in li_keywords:
                # retrieve online keyword
                keyword = self.isogeo.keyword.get(keyword_id=kwd.get("_id"), include=[])
                # associate the metadata with
                self.isogeo.keyword.tagging(
                    metadata=md_dst, keyword=keyword, check_exists=1
                )
            logger.info("{} keywords imported.".format(len(li_keywords)))

        # Limitations (CGUs)
        if len(self.metadata_source.limitations):
            for lim in self.metadata_source.limitations:
                limitation = Limitation(**lim)
                self.isogeo.metadata.limitations.create(
                    metadata=md_dst, limitation=limitation
                )
            logger.info(
                "{} limitations have been imported.".format(
                    len(self.metadata_source.limitations)
                )
            )

        return md_dst

    def import_into_other_metadata(
        self,
        destination_metadata_uuid: str,
        copymark_catalog: str = None,
        copymark_title: bool = True,
        copymark_abstract: bool = True,
        switch_service_layers: bool = False,
        exclude_fields: list = [
            "coordinateSystem",
            "envelope",
            "features",
            "geometry",
            "name",
            "path",
        ],
    ) -> Metadata:
        """Import a metadata content into another one. It can exclude some fields.
        It can apply some copy marks to distinguish the copy from the original.
        
        :param str destination_metadata_uuid: UUID of the metadata to update with source metadata
        :param list exclude_fields: list of fields to be excluded. Must be attributes names
        :param str copymark_catalog: add the new metadata to this additionnal catalog. Defaults to None
        :param bool copymark_title: add a [COPY] mark at the end of the new metadata (default: {True}). Defaults to True
        :param bool copymark_abstract: add a [Copied from](./source_uuid)] mark at the end of the new metadata abstract. Defaults to True
        :param bool switch_service_layers: a service layer can't be associated to many datasetes. \
            If this option is enabled, service layers are removed from the metadata source then added to the new one. Defaults to False


        :returns: the updated Metadata
        :rtype: Metadata

        .. code-block:: python
        
            # TO DO

        """
        # check metadatas UUID
        if not checker.check_is_uuid(destination_metadata_uuid):
            raise ValueError(
                "Destination metadata UUID is not a correct UUID: {}".format(
                    destination_metadata_uuid
                )
            )

        # make a local copy of the source metadata
        md_src = Metadata(**self.metadata_source.to_dict())

        # retrieve the destination metadata - a local bakcup can be useful
        md_dst_bkp = isogeo.metadata.get(destination_metadata_uuid, include="all")

        # additionnal checks
        if md_dst_bkp.type != self.metadata_source.type:
            logger.warning(
                "Trying to import a {} metadata into a {} one. "
                "Let's try but it's at your own risK..."
            )

        # excluding fields (attributes) from the source metadata
        if len(exclude_fields):
            for excluded_attr in exclude_fields:
                if not hasattr(md_src, excluded_attr):
                    logger.error(
                        "Field {} is not a correct attribute of the source metadata. So it can't be excluded...".format(
                            excluded_attr
                        )
                    )
                # if attribute is excluded, then use the original value
                md_src.__setattr__(excluded_attr, getattr(md_dst_bkp, excluded_attr))
                logger.info(
                    "{} attribute original value has been preserved".format(excluded_attr)
                )
            logger.info("{} attributes have been excluded".format(len(exclude_fields)))

        # update the destination metadata with root fields
        md_src._id = destination_metadata_uuid
        md_dst = isogeo.metadata.update(md_src)

        logger.info(
            "Destination metadata has been updated with the root attributes (fields): {} ({}). Let's import the associated resources and subresources.".format(
                md_dst.title, md_dst._id
            )
        )

        # let the API get a rest ;)
        sleep(0.5)


        # NOW PERFORM DUPLICATION OF SUBRESOURCES
        # Catalogs
        li_catalogs_uuids = [
            tag[8:] for tag in md_src.tags if tag.startswith("catalog:")
        ]
        if copymark_catalog is not None:
            li_catalogs_uuids.append(copymark_catalog)

        if len(li_catalogs_uuids):
            for cat_uuid in li_catalogs_uuids:
                # retrieve online catalog
                catalog = self.isogeo.catalog.get(
                    workgroup_id=md_src._creator.get("_id"),
                    catalog_id=cat_uuid,  # CHANGE IT with SDK version >= 3.0.1
                )
                # associate the metadata with
                self.isogeo.catalog.associate_metadata(metadata=md_dst, catalog=catalog)
            logger.info("{} catalogs imported.".format(len(li_catalogs_uuids)))

        # Conditions / Licenses (CGUs)
        if len(md_src.conditions):
            for condition in md_src.conditions:
                licence = License(**condition.get("license"))
                description = condition.get("description")
                self.isogeo.license.associate_metadata(
                    metadata=md_dst, license=licence, description=description, force=0
                )
            logger.info(
                "{} conditions (license + specific description) have been imported.".format(
                    len(md_src.conditions)
                )
            )

        # Contacts
        if len(md_src.contacts):
            for ct in md_src.contacts:
                contact = Contact(**ct.get("contact"))
                self.isogeo.contact.associate_metadata(
                    metadata=md_dst, contact=contact, role=ct.get("role")
                )
            logger.info(
                "{} contacts imported.".format(len(md_src.contacts))
            )

        # Coordinate-systems
        if isinstance(md_src.coordinateSystem, dict):
            srs = CoordinateSystem(**md_src.coordinateSystem)
            self.isogeo.srs.associate_metadata(metadata=md_dst, coordinate_system=srs)
            logger.info("Coordinate-system {} imported.".format(srs.code))

        # Events
        if len(md_src.events):
            for evt in md_src.events:
                event = Event(**evt)
                event.date = event.date[:10]
                self.isogeo.metadata.events.create(metadata=md_dst, event=event)
            logger.info(
                "{} events have been imported.".format(len(md_src.events))
            )

        # Feature attributes
        if md_src.type == "vectorDataset" and len(
            md_src.featureAttributes
        ):
            self.isogeo.metadata.attributes.import_from_dataset(
                metadata_source=self.metadata_source,
                metadata_dest=md_dst,
                mode="update"
            )
            logger.info(
                "{} feature attributes have been imported.".format(
                    len(md_src.featureAttributes)
                )
            )

        # Keywords (including INSPIRE themes)
        li_keywords = self.isogeo.metadata.keywords(self.metadata_source, include=[])
        if len(li_keywords):
            for kwd in li_keywords:
                # retrieve online keyword
                keyword = self.isogeo.keyword.get(keyword_id=kwd.get("_id"), include=[])
                # associate the metadata with
                self.isogeo.keyword.tagging(
                    metadata=md_dst, keyword=keyword, check_exists=1
                )
            logger.info("{} keywords imported.".format(len(li_keywords)))

        # Limitations (CGUs)
        if len(md_src.limitations):
            for lim in md_src.limitations:
                limitation = Limitation(**lim)
                self.isogeo.metadata.limitations.create(
                    metadata=md_dst, limitation=limitation
                )
            logger.info(
                "{} limitations have been imported.".format(
                    len(md_src.limitations)
                )
            )

        # # Links (only URLs)
        # if len(md_src.links):
        #     for lim in md_src.limitations:
        #         limitation = Limitation(**lim)
        #         self.isogeo.metadata.limitations.create(
        #             metadata=md_dst, limitation=limitation
        #         )
        #     logger.info(
        #         "{} limitations have been imported.".format(
        #             len(md_src.limitations)
        #         )
        #     )

        # Service layers associated
        if self.metadata_source.type in ("rasterDataset", "vectorDataset") and len(
            self.metadata_source.serviceLayers
        ):
            if switch_service_layers:
                for service_layer in self.metadata_source.serviceLayers:
                    # remove the layer from the source
                    self.isogeo.metadata.layers.dissociate_metadata(
                        service=Metadata(
                            _id=service_layer.get("service").get("_id"), type="service"
                        ),
                        layer=ServiceLayer(_id=service_layer.get("_id")),
                        dataset=self.metadata_source,
                    )

                    # add the layer to the copy
                    self.isogeo.metadata.layers.associate_metadata(
                        service=Metadata(
                            _id=service_layer.get("service").get("_id"), type="service"
                        ),
                        layer=ServiceLayer(_id=service_layer.get("_id")),
                        dataset=md_dst,
                    )

                logger.info(
                    "{} service layers have been imported after they have been removed from the source.".format(
                        len(self.metadata_source.serviceLayers)
                    )
                )
            else:
                logger.info(
                    "{} service layers have NOT been imported because they stay associated with the source.".format(
                        len(self.metadata_source.serviceLayers)
                    )
                )

        # Specifications
        if len(md_src.specifications):
            for spec in md_src.specifications:
                specification = Specification(**spec.get("specification"))
                isConformant = spec.get("conformant")
                self.isogeo.specification.associate_metadata(
                    metadata=md_dst,
                    specification=specification,
                    conformity=isConformant,
                )
            logger.info(
                "{} specifications have been imported.".format(
                    len(md_src.specifications)
                )
            )

        return md_dst

    # -- DUPLICATION TOOLING -----------------------------------------------------------


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    from logging.handlers import RotatingFileHandler
    from webbrowser import open_new_tab

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

    # # VECTOR duplication
    # md_source = MetadataDuplicator(
    #     isogeo=isogeo,
    #     source_metadata_uuid=environ.get("ISOGEO_METADATA_FIXTURE_UUID"),
    # )
    # new_md = md_source.duplicate_into_same_group(
    #     copymark_catalog="88836154514a45e4b073cfaf350eea02", switch_service_layers=1
    # )
    # print(new_md._id, new_md.title)

    # # SERVICE duplication
    # md_source = MetadataDuplicator(
    #     isogeo=isogeo, source_metadata_uuid="c6989e8b406845b5a86261bd5ef57b60"
    # )
    # new_md = md_source.duplicate_into_same_group(
    #     copymark_catalog="88836154514a45e4b073cfaf350eea02", switch_service_layers=1
    # )
    # print(new_md._id, new_md.title)

    # COPY into another group
    md_source = MetadataDuplicator(
        api_client=isogeo, source_metadata_uuid="b5a66239da6843e1b01e4c6520e87d15"
    )

    new_md = md_source.duplicate_into_other_group(
        destination_workgroup_uuid=environ.get("ISOGEO_WORKGROUP_TEST_UUID")
        # copymark_catalog="88836154514a45e4b073cfaf350eea02",
        # switch_service_layers=1
    )

    # IMPORT into another metadata
    md_source = MetadataDuplicator(
        api_client=isogeo, source_metadata_uuid="5060e12159964063b02717289cd4bb98"
    )

    new_md = md_source.import_into_other_metadata(
        destination_metadata_uuid=new_md._id,
        copymark_catalog="88836154514a45e4b073cfaf350eea02",
        switch_service_layers=1
        )

    
    open_new_tab("https://qa-isogeo-app.azurewebsites.net/groups/f234550ff1d5412fb2c67ee98d826731/resources/" + new_md._id)

    # close connection
    isogeo.close()
