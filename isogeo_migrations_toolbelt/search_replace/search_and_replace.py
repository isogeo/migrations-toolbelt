# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

"""
    Name:         Backup SearchReplaceManager
    Purpose:      Generic module to perform search and replace into metadatas
    Author:       Isogeo

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import logging
import re
from concurrent.futures import ThreadPoolExecutor

# 3rd party
import urllib3

# Isogeo
from isogeo_pysdk import Isogeo, Metadata

# from .updater import MetadataUpdater

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)

# ############################################################################
# ########## Classes #############
# ################################


class SearchReplaceManager(object):
    """Search and replace Manager makes it easy to search into metadata attributes and
    replace existing value by a new one.

    It uses the Isogeo Python SDK to download data asynchronously.

    :param Isogeo api_client: API client authenticated to Isogeo
    :param str objects_kind: API objects type on which to apply the search replace. Defaults to 'metadata'.
    :param dict attributes_patterns: dictionary of metadata attributes and tuple of "value to be replaced", "replacement value".
    :param dict prepositions: dictionary used to manage special cases related to prepositions. \
        Structure: {"preposition to be replaced": "replacement preposition"}
    """

    def __init__(
        self,
        api_client: Isogeo,
        objects_kind: str = "metadata",
        attributes_patterns: dict = {"title": None, "abstract": None},
        prepositions: dict = None,
    ):
        # store API client
        self.isogeo = api_client

        # check object_kind
        if objects_kind != "metadata":
            raise NotImplementedError

        # check parameters of search patterns
        for i in attributes_patterns:
            if not hasattr(Metadata, i):
                raise ValueError("Metadata don't have attribute '{}'".format(i))
        self.attributes_patterns = attributes_patterns

        # prepare prepositions
        self.prepositions = prepositions

    def search_replace(
        self, search_params: dict = {"query": None}, safe: bool = 1
    ) -> dict:
        """It builds a list of metadata to export before transmitting it to an async loop.

        :param dict search params: API client authenticated to Isogeo
        :param bool safe: safe mode enabled or not. In safe mode, the method do not \
            apply modifications online but onyl returns the dictionary with replaced values.

        :returns: dictionary of metadata with replaced values
        :rtype: dict

        :Example:

        .. code-block:: python

        """
        # make the search
        search_params["whole_results"] = True
        metadatas_to_explore = self.isogeo.search(**search_params)
        logger.info("{} metadatas retrieved".format(len(metadatas_to_explore.results)))

        # filter on metadatas matching the given patterns
        metadatas_to_update = self.filter_matching_metadatas(
            metadatas_to_explore.results
        )
        logger.info(
            "{} metadatas matched the patterns and are now ready to be updated.".format(
                len(metadatas_to_update)
            )
        )

        # if safe, just return the results without updating metadata online
        if safe:
            logger.info("Safe mode enabled: Metadata won't be updated online.")
            self.isogeo.close()
            return metadatas_to_update

        # if not safe, launch the update
        with ThreadPoolExecutor(thread_name_prefix="IsogeoSearchReplace") as executor:
            for md in metadatas_to_update:
                logger.info("Metadata sent to update: " + md._id)
                executor.submit(self.isogeo.metadata.update, metadata=md)

    def filter_matching_metadatas(self, isogeo_search_results: list) -> tuple:
        """Filter search results basing on matching patterns.

        :param MetadataSearch isogeo_search_results: Isogeo search results (`MetadataSearch.results`)

        :returns: a tuple of objects with the updated attributes
        :rtype: tuple
        """
        # out list
        di_out_objects = {}

        # parse attributes to replace
        for attribute, pattern in self.attributes_patterns.items():
            logger.info("Searching into '{}' values...".format(attribute))
            # counters
            empty = 0
            ignored = 0
            matched = 0
            # parse metadatas
            for md in isogeo_search_results:
                # load metadata as object
                metadata = Metadata.clean_attributes(md)
                # get attribute value
                in_value = getattr(metadata, attribute)
                # check if attribute has a value
                if not isinstance(in_value, str):
                    empty += 1
                    continue

                # special cases: check if title is different from the technical name
                if attribute == "title" and in_value == metadata.name:
                    empty += 1
                    continue

                # check if the value matches the search
                if pattern[0] in str(in_value):
                    logger.debug(
                        "Value of '{}' to change spotted in {}: '{}'".format(
                            attribute, metadata._id, in_value
                        )
                    )
                    matched += 1

                    if metadata._id in di_out_objects:
                        # object has already been previously updated
                        updated_obj = di_out_objects.get(metadata._id)
                        # apply replacement
                        setattr(
                            updated_obj, attribute, self.replacer(in_value, pattern)
                        )
                        di_out_objects[metadata._id] = updated_obj
                    else:
                        setattr(metadata, attribute, self.replacer(in_value, pattern))
                        di_out_objects[metadata._id] = metadata
                else:
                    ignored += 1

            # log for this attribute
            logger.info(
                "{} metadatas do not contains a valid {}".format(empty, attribute)
            )
            logger.info(
                "{} metadatas.{} DO NOT MATCH the pattern: {}".format(
                    ignored, attribute, pattern[0]
                )
            )
            logger.info(
                "{} metadatas.{} MATCH the pattern: {}".format(
                    matched, attribute, pattern[0]
                )
            )

        # return tuple of metadata to be updated
        return tuple(di_out_objects.values())

    def replacer(self, in_text: str, pattern: tuple) -> str:
        """Filter search results basing on matching patterns.

        :param str in_text: text into search a match
        :param tuple pattern: tuple of str ("to be replaced", "replacement")
        """

        if self.prepositions is None:
            # handling meta caracters into patterns' elements
            li_meta_car = [".", "^", "$", "*", "+", "?", "{", "}", "[", "]", "|", "(", ")"]
            if all(car not in pattern[0] for car in li_meta_car) and all(car not in pattern[1] for car in li_meta_car):
                return re.sub(
                    pattern=r"({}+)".format(pattern[0]), repl=pattern[1], string=in_text
                )
            else:
                return in_text.replace(pattern[0], pattern[1])
        else:
            # if prepositions are set, so apply them first
            out_text = in_text
            for in_prep, new_prep in self.prepositions.items():
                # logger.info("Pattern applied: {}".format(r"({}{}+)".format(in_prep, pattern[0])))
                # logger.info("Replacement applied: {}".format("{}{}".format(new_prep, pattern[1])))
                out_text = re.sub(
                    pattern="({}{}+)".format(in_prep, pattern[0]),
                    repl="{}{}".format(new_prep, pattern[1]),
                    string=out_text,
                )
            # then apply basic pattern

            return re.sub(
                pattern=r"({}+)".format(pattern[0]), repl=pattern[1], string=out_text
            )


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    # additional imports
    from logging.handlers import RotatingFileHandler
    from os import environ

    # 3rd party
    from dotenv import load_dotenv

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

    # environment vars
    load_dotenv("prod.env", override=True)

    # ignore warnings related to the QA self-signed cert
    if environ.get("ISOGEO_PLATFORM").lower() == "qa":
        urllib3.disable_warnings()

    # establish isogeo connection
    isogeo = Isogeo(
        client_id=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_LEGACY_CLIENT_SECRET"),
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
        api_client=isogeo, attributes_patterns=replace_patterns
    )

    # prepare search parameters
    search_parameters = {"group": "542bc1e743f6464fb471dc48f0da02d2"}

    # launch search and replace
    searchrpl_mngr.search_replace(search_params=search_parameters, safe=1)

    # close connection
    isogeo.close()
