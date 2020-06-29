# -*- coding: UTF-8 -*-
#! python3

"""
    Name:         Script to create matching table for Mayenne migration
    Author:       Isogeo
    Purpose:      Script using the migrations-toolbelt package to perform metadata migration.
                Logs are willingly verbose.
    Python:       3.7+
"""

# ##############################################################################
# ########## Libraries #############

# Standard Library
import csv
import datetime
from timeit import default_timer
from os import environ
from pathlib import Path

# 3rd party
from dotenv import load_dotenv

# Isogeo
from isogeo_pysdk import (
    Isogeo,
    IsogeoChecker,
)

checker = IsogeoChecker()
# load .env file
load_dotenv("./env/mayenne.env", override=True)

if __name__ == "__main__":
    li_origin_wg_uuid = environ.get("ISOGEO_ORIGIN_WORKGROUP_OTHER").split(";")
    # ############################### LOADING SOURCE AND TARGET METADATAS INFOS ###############################
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

    li_for_csv = []
    date_ref = datetime.datetime(2020, 5, 8)
    for wg_uuid in li_origin_wg_uuid:
        if default_timer() - auth_timer >= 250:
            print("Manually refreshing token")
            isogeo.connect(
                username=environ.get("ISOGEO_USER_NAME"),
                password=environ.get("ISOGEO_USER_PASSWORD"),
            )
            auth_timer = default_timer()
        else:
            pass
        # request Isogeo API about source metadatas
        md_search = isogeo.search(
            group=wg_uuid,
            whole_results=True
        )
        wg_name = md_search.results[0].get("_creator").get("contact").get("name")
        wg_status = ""
        print("\n{} source metadatas retrieved from '{}' workgroup ({})".format(md_search.total, wg_name, wg_uuid))
        li_md_src = []
        li_md_trg = []
        li_name_trg = []
        li_name_trg_low = []
        nb_matched = 0
        for md in md_search.results:
            str_date = md.get("_created").split("T")[0]
            creation_date = datetime.datetime.strptime(str_date, "%Y-%m-%d")
            if creation_date < date_ref:
                li_md_src.append((md.get("_id"), md.get("title"), md.get("name", "NR")))
            else:
                li_md_trg.append((md.get("_id"), md.get("name")))
                li_name_trg.append(md.get("name"))
                li_name_trg_low.append(md.get("name").lower())

        print("{} source metadata retrieved".format(len(li_md_src)))
        print("{} target metadata retrieved".format(len(li_md_trg)))
        if len(li_md_trg) == 0:
            wg_status = "no_target_found"
        else:
            wg_status = "nothing_special"

        for md_src in li_md_src:
            if md_src[2] != "NR" and wg_status != "no_target_found":
                if md_src[2] in li_name_trg:
                    index_trg = li_name_trg.index(md_src[2])
                    md_trg = li_md_trg[index_trg]

                    li_for_csv.append(
                        [
                            md_src[0],
                            md_src[1],
                            md_src[2],
                            md_trg[1],
                            md_trg[0],
                            "perfect",
                            wg_uuid,
                            wg_name,
                            wg_status

                        ]
                    )
                    nb_matched += 1

                elif md_src[2].lower() in li_name_trg_low:
                    index_trg = li_name_trg_low.index(md_src[2].lower())
                    md_trg = li_md_trg[index_trg]

                    li_for_csv.append(
                        [
                            md_src[0],
                            md_src[1],
                            md_src[2],
                            md_trg[1],
                            md_trg[0],
                            "incassable",
                            wg_uuid,
                            wg_name,
                            wg_status

                        ]
                    )
                    nb_matched += 1

                else:
                    li_for_csv.append(
                        [
                            md_src[0],
                            md_src[1],
                            md_src[2],
                            "NR",
                            "NR",
                            "NULL",
                            wg_uuid,
                            wg_name,
                            wg_status
                        ]
                    )
            else:
                li_for_csv.append(
                    [
                        md_src[0],
                        md_src[1],
                        md_src[2],
                        "NR",
                        "NR",
                        "impossible",
                        wg_uuid,
                        wg_name,
                        wg_status
                    ]
                )
        if nb_matched == len(li_md_trg) and wg_status != "no_target_found":
            for line in li_for_csv:
                if line[6] == wg_uuid and line[8] != "no_target_found":
                    line[8] = "all_target_matched"
                else:
                    pass
        print("{} on {} source metadata have matched with a target".format(nb_matched, len(li_md_src)))

    isogeo.close()

    csv_path = Path(r"./scripts/mayenne/csv/correspondances_other.csv")
    with open(file=csv_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        writer.writerow(
            [
                "source_uuid",
                "source_title",
                "source_name",
                "target_name",
                "target_uuid",
                "match_type",
                "workgroup_uuid",
                "workgroup_name",
                "workgroup_status"
            ]
        )
        for data in li_for_csv:
            writer.writerow(data)
