# -*- coding: UTF-8 -*-
#! python3  # noqa: E265

"""
    Usage from the repo root folder:

        python -m migrate_from_excel

"""

# #############################################################################
# ########## Libraries #############
# ##################################

# Standard library
import csv
import logging

# #############################################################################
# ######## Globals #################
# ##################################

# logs
logger = logging.getLogger(__name__)

# ############################################################################
# ########## Classes #############
# ################################


class CsvReader(csv.DictReader):
    """[summary]

    Arguments:
        csv {[type]} -- [description]
    """

    def __init__(self, filename):
        """[summary]

        Arguments:
            filename {[type]} -- [description]
        """
        self.__fo = open(filename, "rb")
        self.__delim = ";"
        self.rows = csv.DictReader(self.__fo, self.__delim)
        self.rows.__init__(self.__fo, self.__delim)

    def close(self):
        self.__fo.close()


# ##############################################################################
# ##### Stand alone program ########
# ##################################
if __name__ == "__main__":
    csv_reader = CsvReader("./duplicate/input/magosm_matching_table.csv")
    for i in csv_reader.rows:
        print(i)
