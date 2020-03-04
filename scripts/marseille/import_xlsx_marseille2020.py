# ##################### IMPORT #####################
# Standard library
import logging
from logging.handlers import RotatingFileHandler
from os import environ
from pathlib import Path
from timeit import default_timer
from pprint import pprint
import sys

# 3rd party
import urllib3
from dotenv import load_dotenv
from openpyxl import load_workbook

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

from isogeotoxlsx.i18n import I18N_FR
from isogeotoxlsx.matrix import VECTOR_COLUMNS
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
    Path("./try/_logs/read_xlsx.log"), "a", 5000000, 1
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
path_input_excel = Path(environ.get("EXCEL_FILE_PATH"))
group = ""
platform = environ.get("ISOGEO_PLATFORM")

if platform.lower() == "qa":
    urllib3.disable_warnings()

# ##################### MAIN #####################
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

logger.info("------------------------- READING EXCEL FILE ----------------------------")
xlsx_reader = IsogeoFromxlsx(environ.get("EXCEL_FILE_PATH"))
xlsx_reader.read_file()


xlsx_md_uuids = [record.get("md")._id for record in xlsx_reader.md_read]
search = isogeo.search(
    include="all", whole_results=True, specific_md=tuple(xlsx_md_uuids)
)

dict_cat_kw = {
    "test_1": "d8d24a8306f44cc093c3522c2219d33a",
    "test_2": "41245bbed2ad493cb01f5903d1757eeb",
}

if xlsx_reader.lang == "fr":
    dict_inspireTh = dict_inspire_fr
    isogeo_tr = IsogeoTranslator("FR")
else:
    dict_inspireTh = dict_inspire_en
    isogeo_tr = IsogeoTranslator("EN")

logger.info("-------------------- BROWSE TROUGH EXCEL METADATAS -------------------")
if len(search.results) == len(xlsx_reader.md_read):

    for record in xlsx_reader.md_read:
        # retrieve xlsx infos
        xlsx_md = record.get("md")
        logger.info(
            "\n------------- Update isogeo md from {} xlsx one ---------------".format(
                xlsx_md._id
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
                [md for md in search.results if md.get("_id") == xlsx_md._id][0]
            )
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
        except Exception as e:
            logger.error("faile to retrieve isogeo_md : {}".format(e))
            continue

        # retire right format for xlsx_md from isogeo md tags
        if xlsx_md.format and xlsx_md.format.split(" (")[0] in list(
            isogeo_md.tags.values()
        ):
            format_tag = [
                k for k, v in isogeo_md.tags.items() if str(v) in xlsx_md.format
            ][0]
            xlsx_md.format = format_tag.split(":")[1]
        else:
            pass
        # retire right updateFrequency for xlsx_md from isogeo translator dict
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
            for attr in isogeo_md.ATTR_CREA:
                xlsx_value = getattr(xlsx_md, attr)
                isogeo_value = getattr(isogeo_md, attr)
                if str(xlsx_value) != str(isogeo_value):
                    if xlsx_value:
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
            isogeo.metadata.update(isogeo_md)
            # KEYWORDS
            logger.info("Update KEYWORDS")
            for kw in xlsx_kws:
                if kw.text not in isogeo_kws:
                    if kw.text in list(dict_cat_kw.keys()):
                        cat_kw_uuid = dict_cat_kw.get(kw.text)
                        cat_kw = isogeo.catalog.get(
                            workgroup_id=environ.get("ISOGEO_WORKGROUP_TEST_UUID"),
                            catalog_id=cat_kw_uuid,
                        )
                        isogeo.catalog.associate_metadata(
                            metadata=isogeo_md, catalog=cat_kw
                        )
                    else:
                        new_kw = isogeo.keyword.create(kw)
                        if new_kw.text == kw:
                            isogeo.keyword.tagging(
                                metadata=isogeo_md, keyword=new_kw, check_exists=1
                            )
                        else:
                            nb_candidate_kw = isogeo.keyword.thesaurus(
                                query=kw.text, page_size=1
                            ).total
                            logger.info(
                                "{} keywords are matching with '{}' trying to retrieve the exact one".format(
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
                                    if isogeo_kw.get("text") == kw.text
                                ][0]
                            )
                            isogeo.keyword.tagging(
                                metadata=isogeo_md, keyword=good_kw, check_exists=1
                            )
                else:
                    pass
            # CONTACTS
            logger.info("Update CONTACTS")
            for contact in xlsx_contacts:
                if contact.name not in isogeo_contacts:
                    try:
                        new_contact = isogeo.contact.create(
                            workgroup_id=environ.get("ISOGEO_WORKGROUP_TEST_UUID"),
                            contact=contact,
                            check_exists=1,
                        )
                        isogeo.contact.associate_metadata(
                            metadata=isogeo_md,
                            contact=new_contact,
                            role="pointOfContact",
                        )
                    except Exception as e:
                        logger.info(
                            "faile to associate {} contact to {} md : {}".format(
                                contact.name, isogeo_md._id, e
                            )
                        )
                else:
                    pass
            # INSPIRE
            logger.info("Update INSPIRE THEMES")
            for inspireTh_label, inspireTh_uuid in xlsx_inspire.items():
                if inspireTh_label in isogeo_inspireTh:
                    pass
                else:
                    th = isogeo.keyword.get(keyword_id=inspireTh_uuid)
                    isogeo.keyword.tagging(metadata=isogeo_md, keyword=th)

        else:
            logger.info(
                "matching issue for {} uuid that point to {} data in Isogeo and to {} data in xlsx file".format(
                    isogeo_md._id, isogeo_md.name, xlsx_md.name
                )
            )
else:
    logger.info("Some metadata exists in xlsx file but not in Isogeo")

isogeo.close()
