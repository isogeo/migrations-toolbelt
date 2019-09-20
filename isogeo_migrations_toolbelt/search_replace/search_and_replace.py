# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Backup Manager
    Purpose:      Generic module to perform backup from Isogeo
    Author:       Isogeo

    Python:       3.6+
"""

# ##############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import asyncio
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import sleep

# 3rd party
import urllib3

# Isogeo
from isogeo_pysdk import Isogeo, Metadata, MetadataSearch
from isogeo_pysdk.checker import IsogeoChecker

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)
checker = IsogeoChecker()

# ############################################################################
# ########## Classes #############
# ################################


class SearchReplaceManager(object):
    """Search and replace Manager makes it easy to search into metadata attributes and
    replace existing value by a new one.
    
    It uses the Isogeo Python SDK to download data asynchronously.
    
    :param Isogeo api_client: API client authenticated to Isogeo
    :param str output_folder: path to the folder where to store the sample
    :param str objects_kind: API objects type on which to apply the search replace. Defaults to 'metadata'.
    :param dict attributes_patterns: dictionary of metadata attributes and tuple of "value to be replaced", "replacement value".
    :param dict prepositions: dictionary used to manage special cases related to prepositions. \
        Structure: {"preposition to be replaced": "replacement preposition"}
    """

    def __init__(
        self,
        api_client: Isogeo,
        output_folder: str,
        objects_kind: str = "metadata",
        attributes_patterns: dict = {"title": None, "abstract": None},
        prepositions: dict = None,
    ):
        # store API client
        self.isogeo = api_client

        # check object_kind
        if objects_kind != "metadata":
            raise NotImplementedError

        # output folder
        self.outfolder = Path(output_folder)
        if not self.outfolder.exists():
            self.outfolder.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Given output folder doesn't exist. It has been created: {}".format(
                    self.outfolder.resolve()
                )
            )
        if not self.outfolder.is_dir():
            raise TypeError(
                "'output_folder' expect a folder path. Given: {}".format(
                    self.outfolder.resolve()
                )
            )

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
        """
        
        It builds a list of metadata to export before transmitting it to an async loop. 
        
        :param dict search params: API client authenticated to Isogeo
        :param bool safe: safe mode enabled or not. In safe mode, the method do not 
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

        print(metadatas_to_explore.results)

        # self.li_api_routes = []
        # for i in search_to_export.results:
        #     # ensure final folder exists
        #     final_dest = self.outfolder.resolve() / i.get("_creator").get("_id")
        #     final_dest.mkdir(parents=True, exist_ok=True)

        #     # build the list of methods to execute
        #     self.li_api_routes.append(
        #         {
        #             "route": self.isogeo.metadata.get,
        #             "params": {"metadata_id": i.get("_id"), "include": "all"},
        #             "output_json_name": "{}/{}".format(
        #                 i.get("_creator").get("_id"), i.get("_id")
        #             ),
        #         }
        #     )

        # # async loop
        # loop = asyncio.get_event_loop()
        # future = asyncio.ensure_future(self._export_metadata_asynchronous())
        # loop.run_until_complete(future)

        # return True

    def filter_matching_metadatas(self, isogeo_search_results: list) -> tuple:
        """Filter search results basing on matching patterns.
        
        :param MetadataSearch isogeo_search_results: Isogeo search results (`MetadataSearch.results`)

        :returns: a tuple of objects with the updated attributes
        :rtype: tuple
        """
        # out list
        li_out_objects = []

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
                if str(pattern[0]) in str(in_value):
                    logger.debug(
                        "Value to change spotted in {}: '{}'".format(
                            metadata._id, in_value
                        )
                    )
                    matched += 1
                    # create new object with
                    updated_obj = Metadata(_id=metadata._id)
                    # apply replacement
                    setattr(updated_obj, attribute, self.replacer(in_value, pattern))
                    # add it to the output iterable
                    li_out_objects.append(updated_obj)
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
        return tuple(li_out_objects)

    def replacer(self, in_text: str, pattern: tuple) -> str:
        """Filter search results basing on matching patterns.
        
        :param str in_text: text into search a match
        :param tuple pattern: tuple of str ("to be replaced", "replacement")
        """
        if self.prepositions is None:
            return re.sub(
                pattern=r"({}+)".format(pattern[0]), repl=pattern[1], string=in_text
            )
        else:
            out_text = in_text
            for in_prep, new_prep in self.prepositions.items():
                # logger.info("Pattern applied: {}".format(r"({}{}+)".format(in_prep, pattern[0])))
                # logger.info("Replacement applied: {}".format("{}{}".format(new_prep, pattern[1])))
                out_text = re.sub(
                    pattern="({}{}+)".format(in_prep, pattern[0]),
                    repl="{}{}".format(new_prep, pattern[1]),
                    string=out_text,
                )
            return out_text

    # def _store_to_json(self, func_outname_params: dict):
    #     """Meta function meant to be executed in async mode.
    #     In charge to make the request to the Isogeo API and store the result into a JSON file.

    #     :param dict func_outname_params: parameters for the execution. Expected structure:

    #         .. code-block:: python

    #             {
    #                 "route": self.isogeo.metadata.get,
    #                 "params": {"metadata_id": METADATA_UUID, "include": "all"},
    #                 "output_json_name": "{}/{}".format(WORKGROUP_UUID, METADATA_UUID),
    #             }

    #     """
    #     route_method = func_outname_params.get("route")
    #     out_filename = Path(
    #         self.outfolder.resolve(),
    #         func_outname_params.get("output_json_name") + ".json",
    #     )

    #     try:
    #         # use request
    #         request = route_method(**func_outname_params.get("params"))
    #         # transform objects into dicts
    #         if not isinstance(request, (dict, list)):
    #             request = request.to_dict()
    #         # store response into a json file
    #         with out_filename.open("w") as out_json:
    #             json.dump(
    #                 obj=request, fp=out_json, sort_keys=True, indent=4, default=str
    #             )
    #     except Exception as e:
    #         logger.error(
    #             "Export failed to '{output_json_name}.json' "
    #             "using route '{route}' "
    #             "with these params '{params}'".format(**func_outname_params)
    #         )
    #         logger.error(e)

    # # -- ASYNC METHODS -----------------------------------------------------------------
    # async def _export_metadata_asynchronous(self):
    #     """Async loop builder."""
    #     with ThreadPoolExecutor(
    #         max_workers=5, thread_name_prefix="IsogeoBackupManager_"
    #     ) as executor:
    #         # Set any session parameters here before calling `fetch`
    #         loop = asyncio.get_event_loop()
    #         tasks = [
    #             loop.run_in_executor(
    #                 executor,
    #                 self._store_to_json,
    #                 # Allows us to pass in multiple arguments to `fetch`
    #                 *(api_route,),
    #             )
    #             for api_route in self.li_api_routes
    #         ]

    #         # store responses
    #         out_list = []
    #         for response in await asyncio.gather(*tasks):
    #             out_list.append(response)

    #         return out_list


# #############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    """Standalone execution for quick and dirty use or test"""
    # additional imports
    from logging.handlers import RotatingFileHandler
    from os import environ
    from webbrowser import open_new_tab

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
        client_id=environ.get("ISOGEO_API_USER_CLIENT_ID"),
        client_secret=environ.get("ISOGEO_API_USER_CLIENT_SECRET"),
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
        api_client=isogeo,
        output_folder="./_output/search_replace/",
        attributes_patterns=replace_patterns,
    )

    # prepare search parameters
    search_parameters = {"group": "542bc1e743f6464fb471dc48f0da02d2"}

    # launch search and replace
    searchrpl_mngr.search_replace(search_params=search_parameters, safe=1)

    # close connection
    isogeo.close()
