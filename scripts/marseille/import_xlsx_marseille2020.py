# ##################### IMPORT #####################
# Standard library
import logging
import time
from timeit import default_timer
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
from pprint import pprint
import sys

# 3rd party
import urllib3
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
    Metadata,
    Keyword,
    Limitation,
    License,
    Specification,
    IsogeoTranslator,
    Contact,
    ApiKeyword,
)

sys.path.append(str(Path(__file__).parents[2]))
from isogeotoxlsx import IsogeoFromxlsx, dict_inspire_fr, dict_inspire_en

checker = IsogeoChecker()

# ##################### SETTINGS #####################
load_dotenv("dev.env")
# logs
logger = logging.getLogger()
# ------------ Log & debug ----------------
logging.captureWarnings(True)
logger.setLevel(logging.DEBUG)
# logger.setLevel(logging.INFO)

log_format = logging.Formatter(
    "%(asctime)s || %(levelname)s "
    "|| %(module)s - %(lineno)d ||"
    " %(funcName)s || %(message)s"
)

# debug to the file
log_file_handler = RotatingFileHandler(
    Path("./try/_logs/import_marseille.log"), "a", 5000000, 1
)
log_file_handler.setLevel(logging.DEBUG)
log_file_handler.setFormatter(log_format)

# info to the console
log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.INFO)
log_console_handler.setFormatter(log_format)

logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)


# params
path_input_excel = Path("{}".format(environ.get("EXCEL_FILE_PATH"))).resolve()
group = ""
platform = environ.get("ISOGEO_PLATFORM")

if platform.lower() == "qa":
    urllib3.disable_warnings()

# ##################### MAIN #####################
logger.info("\n########################### IMPORT SESSION ###########################\n")
dict_cat_kw = {
    "CT1": "8c554dc2e57742b0addf44d9ab108913",
    "CT2": "048d9f77f2834477a48394a214791c87",
    "CT3": "dbb968024e3e42fdafcda3302eef49c5",
    "DIC": "972f6915d8d74cb7b239604fad8e7cd4",
    "CT4": "7f578b337fcc40e28b371fe0c2fffcef",
    "Référentiels": "003612cf5a164c39b2facb289351aeac",
}

tup_nogo_attr = (
    "envelope",
    "encoding",
    "features",
    "format",
    "formatVersion",
    "geometry",
    "path",
    "name",
    "type",
    "editionProfile",
    "series",
)

logger.info("------------------------- READING EXCEL FILE ----------------------------")
xlsx_reader = IsogeoFromxlsx(environ.get("EXCEL_FILE_PATH"))
xlsx_reader.read_file()

xlsx_md_uuids = [record.get("md")._id for record in xlsx_reader.md_read]

if xlsx_reader.lang == "fr":
    dict_inspireTh = dict_inspire_fr
    isogeo_tr = IsogeoTranslator("FR")
else:
    dict_inspireTh = dict_inspire_en
    isogeo_tr = IsogeoTranslator("EN")

logger.info("--------------------- INSTANCIATING ISOGEO CLIENT -----------------------")
# API client instanciation
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
auth_timer = default_timer()

logger.info("Retrieving keywords catalogs")
for cat in dict_cat_kw:
    isogeo_cat = isogeo.catalog.get(
        workgroup_id=environ.get("ISOGEO_WORKGROUP_USER_UUID"),
        catalog_id=dict_cat_kw.get(cat),
    )
    dict_cat_kw[cat] = isogeo_cat


logger.info("--------------------- RETRIEVING ISOGEO METADATAS -----------------------")
isogeo_mds = []

amplitude = 150
bound_range = int(len(xlsx_reader.md_read) / amplitude)
li_bound = []
for i in range(bound_range + 1):
    li_bound.append(amplitude * i)
li_bound.append(len(xlsx_reader.md_read))

logger.info("Starting requesting Isogeo API for {} rounds".format(len(li_bound) - 1))
for i in range(len(li_bound) - 1):
    bound_inf = li_bound[i]
    bound_sup = li_bound[i + 1]
    logger.info(
        "Round {} - retrieve from metdata {} to {}".format(
            i + 1, bound_inf + 1, bound_sup
        )
    )

    li_uuid = tuple(xlsx_md_uuids[bound_inf:bound_sup])
    try:
        search_results = isogeo.search(
            include="all", whole_results=True, specific_md=tuple(li_uuid)
        ).results
        isogeo_mds += search_results
    except Exception as e:
        logger.info("an error occured : {}".format(e))

logger.info(
    "Retrieving '{}' special catalog".format(environ.get("ISOGEO_IMPORT_CATALOG_UUID"))
)
special_cat = isogeo.catalog.get(
    workgroup_id=environ.get("ISOGEO_WORKGROUP_USER_UUID"),
    catalog_id=environ.get("ISOGEO_IMPORT_CATALOG_UUID"),
)

logger.info("{} Isogeo metadatas retrieved".format(len(isogeo_mds)))

logger.info("-------------------- BROWSE TROUGH EXCEL METADATAS -------------------")
li_keywords = []
# if len(search.results) == len(xlsx_reader.md_read):
for record in xlsx_reader.md_read[390:]:
    logger.debug("{}s remaining before token expiration".format(299 - (default_timer() - auth_timer)))
    if default_timer() - auth_timer >= 250:
        logger.info("Manually refreshing token")
        isogeo.connect(
            username=environ.get("ISOGEO_USER_NAME"),
            password=environ.get("ISOGEO_USER_PASSWORD"),
        )
        auth_timer = default_timer()
    else:
        pass
    isogeo.token.get("expires_in")
    # retrieve xlsx infos
    xlsx_md = record.get("md")
    logger.info(
        "\n------------- Update isogeo md from {} xlsx one ({}/{}) ---------------".format(
            xlsx_md._id, xlsx_reader.md_read.index(record) + 1, len(xlsx_reader.md_read)
        )
    )
    logger.info("Retrieving xlsx infos")
    xlsx_contacts = record.get("contacts")
    xlsx_kws = record.get("keywords")
    xlsx_inspire = record.get("inspireThemes")
    xlsx_events = record.get("events")

    try:
        logger.info("Retrieving isogeo infos")
        # retrieve isogeo md
        isogeo_md = Metadata().clean_attributes(
            [md for md in isogeo_mds if md.get("_id") == xlsx_md._id][0]
        )
        origin_md = isogeo_md
        isogeo_contacts = [
            v for k, v in isogeo_md.tags.items() if k.startswith("contact:")
        ]
        isogeo_kws = [
            v for k, v in isogeo_md.tags.items() if k.startswith("keyword:is")
        ]
        isogeo_inspireTh = [
            v.strip()
            for k, v in isogeo_md.tags.items()
            if k.startswith("keyword:in")
        ]
        isogeo_catalogs_uuid = [
            k.split(":")[1] for k in isogeo_md.tags if k.startswith("catalog")
        ]
    except Exception as e:
        logger.error("faile to retrieve isogeo_md : {}".format(e))
        continue

    # retrieve right format for xlsx_md from isogeo md tags
    if xlsx_md.format and xlsx_md.format.split(" (")[0] in list(
        isogeo_md.tags.values()
    ):
        format_tag = [
            k for k, v in isogeo_md.tags.items() if str(v) in xlsx_md.format
        ][0]
        xlsx_md.format = format_tag.split(":")[1]
    else:
        pass
    # retrieve right updateFrequency for xlsx_md from isogeo translator dict
    dict_frequency_trad = isogeo_tr.translations.get("frequencyShortTypes")
    if xlsx_md.updateFrequency and xlsx_md.updateFrequency.split(" ")[-1] in list(dict_frequency_trad.values()):
        xlsx_updateFrequency_items = xlsx_md.updateFrequency.split(" ")
        time_unit = [k for k, v in dict_frequency_trad.items() if v == xlsx_updateFrequency_items[-1]][0]
        period_value = xlsx_updateFrequency_items[-2]
        updateFrequency_prefix = ""
        if time_unit == "H":
            updateFrequency_prefix = "PT"
        else:
            updateFrequency_prefix = "P"
        xlsx_md.updateFrequency = "{}{}{}".format(updateFrequency_prefix, period_value, time_unit)
    else:
        if xlsx_md.updateFrequency:
            xlsx_md.updateFrequency = None
            logger.info("This update frequency format value can't be parsed to fit isogeo format : '{}'".format(xlsx_md.updateFrequency))
        else:
            pass

    if xlsx_md.name == isogeo_md.name:
        # ROOT_ATTRIBUTES
        diff_count = 0
        for attr in isogeo_md.ATTR_CREA:
            if attr not in tup_nogo_attr:
                xlsx_value = getattr(xlsx_md, attr)
                isogeo_value = getattr(isogeo_md, attr)
                if str(xlsx_value) != str(isogeo_value):
                    if xlsx_value:
                        diff_count += 1
                        logger.info(
                            "{} --> {} VS {}".format(attr, xlsx_value, isogeo_value)
                        )
                        setattr(isogeo_md, attr, xlsx_value)
                    else:
                        logger.debug(
                            "no value set in xlsx file for {} field of {} metadata".format(
                                attr, isogeo_md._id
                            )
                        )
                        pass
                # nothing has changed
                else:
                    pass
            else:
                pass
        if diff_count != 0:
            isogeo.metadata.update(isogeo_md)
        else:
            pass
        # add metadata to special catalog if needed
        # if special_cat not in isogeo_catalogs_uuid:
        #     isogeo.catalog.associate_metadata(metadata=isogeo_md, catalog=special_cat)
        # else:
        #     pass
        # KEYWORDS
        logger.info("Update KEYWORDS")
        for kw in xlsx_kws:
            if kw.text not in isogeo_kws:
                # for keywords coresponding to catalogs, associating the catalog to the metadata
                if kw.text in list(dict_cat_kw.keys()) and dict_cat_kw.get(kw.text)._id not in isogeo_catalogs_uuid:
                    cat_kw_uuid = dict_cat_kw.get(kw.text)._id
                    cat_kw = dict_cat_kw.get(kw.text)
                    isogeo.catalog.associate_metadata(
                        metadata=isogeo_md, catalog=cat_kw
                    )
                else:
                    pass
                # retrieving the list of saved keywords labels
                li_kw_labels = [keyword.text for keyword in li_keywords]
                if kw.text not in li_kw_labels:
                    new_kw = isogeo.keyword.create(kw)
                    if new_kw.text.lower() == kw.text.lower():
                        isogeo.keyword.tagging(
                            metadata=isogeo_md, keyword=new_kw, check_exists=1
                        )
                        li_keywords.append(new_kw)
                        pass
                    else:
                        nb_candidate_kw = isogeo.keyword.thesaurus(
                            query=kw.text, page_size=1
                        ).total
                        logger.info(
                            "{} keywords are matching with '{}' retrieving the exact one".format(
                                nb_candidate_kw, kw.text
                            )
                        )
                        candidate_kw = isogeo.keyword.thesaurus(
                            query=kw.text, page_size=nb_candidate_kw
                        ).results
                        good_kw = Keyword(
                            **[
                                isogeo_kw
                                for isogeo_kw in candidate_kw
                                if isogeo_kw.get("text").lower() == kw.text.lower()
                            ][0]
                        )
                        logger.info("'{}' keyword retrieved, let's associate it".format(good_kw.text))
                        isogeo.keyword.tagging(
                            metadata=isogeo_md, keyword=good_kw, check_exists=1
                        )
                        li_keywords.append(good_kw)
                # if this keyword was already tagged, no need to try to create it, just retrieve it in list of saved keywords and tagg the metadata
                else:
                    logger.debug("'{}' keyword saved because already tagged, no need to try to create id".format(kw.text))
                    good_kw = [keyword for keyword in li_keywords if keyword.text == kw.text][0]
                    isogeo.keyword.tagging(
                        metadata=isogeo_md, keyword=good_kw, check_exists=1
                    )
                    pass
            # metadata already tagged with this keyword
            else:
                pass
        # CONTACTS
        # logger.info("Update CONTACTS")
        # for contact in xlsx_contacts:
        #     if contact.name not in isogeo_contacts:
        #         try:
        #             new_contact = isogeo.contact.create(
        #                 workgroup_id=environ.get("ISOGEO_WORKGROUP_TEST_UUID"),
        #                 contact=contact,
        #                 check_exists=1,
        #             )
        #             # isogeo.contact.associate_metadata( ###############################
        #             #     metadata=isogeo_md,
        #             #     contact=new_contact,
        #             #     role="pointOfContact",
        #             # )
        #         except Exception as e:
        #             logger.info(
        #                 "faile to associate {} contact to {} md : {}".format(
        #                     contact.name, isogeo_md._id, e
        #                 )
        #             )
        #     else:
        #         pass
        # INSPIRE
        logger.info("Update INSPIRE THEMES")
        for inspireTh_label, inspireTh_uuid in xlsx_inspire.items():
            if inspireTh_label not in isogeo_inspireTh:
                th = isogeo.keyword.get(keyword_id=inspireTh_uuid)
                isogeo.keyword.tagging(metadata=isogeo_md, keyword=th) #####################################
            else:
                pass

        # EVENTS
        # logger.info("Update EVENTS")
        # for event in xlsx_events:
        #     event = event

        # CONDITIONS

        # LIMITATIONS

        # SPECIFICATIONS

    else:
        logger.info(
            "matching issue for {} uuid that point to {} data in Isogeo and to {} data in xlsx file".format(
                isogeo_md._id, isogeo_md.name, xlsx_md.name
            )
        )
# else:
#     logger.info("Some metadata exists in xlsx file but not in Isogeo")

isogeo.close()
