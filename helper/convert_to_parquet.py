#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2018 Eli Lilly and Company
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Converts files into snappy-parquet without using Spark and JVM. This is so they
are compressed before putting into HDFS and/or before Spark reads them to
accelerate all these tasks.
"""

from __future__ import print_function, unicode_literals
import os
import sys
import logging
import argparse
from io import open
from multiprocessing import Pool

import pandas
import fastparquet # also need python-snappy installed.


class Conversion:
    """
    Convert file to parquet.
    """

    def __init__(self, input_name, output_name, delimiter):
        self.input_name = input_name
        self.output_name = output_name
        self.delimiter = delimiter

    def read(self):
        """
        Read the files into Pandas dataframe, required by fastparquet. Reads
        based on delimiter, faster to explicitly switch between these types
        than to detect since it uses the c engine instead of python to read.

        :return: pandas.dataframe()
        """
        with open(self.input_name, 'r', encoding='utf-8') as file_handle:
            if self.delimiter == 'tab':
                data_frame = pandas.read_table(file_handle, index_col=False)
            elif self.delimiter == 'comma':
                data_frame = pandas.read_csv(file_handle, index_col=False)
        return data_frame

    def write(self, dataframe):
        """
        Write a pandas dataframe into parquet format using fastparquet and
        snappy compression.

        :param dataframe: a pandas dataframe as input
        """
        fastparquet.write(self.output_name, dataframe, compression='SNAPPY')
        logging.info("Parquet format at: %s", self.output_name)

    def check(self):
        """
        Check if the output file has been written before already. During
        recovery or re-run, don't waste time by re-writing the same
        files.

        :return: bool if path exists
        """
        if os.path.isfile(self.output_name):
            return True

    def execute(self):
        """
        Check the file's existence and convert into parquet if the output
        does not already exist. Public method to call on the class.
        """
        if self.check():
            logging.error("File %s already exists!", self.output_name)
        else:
            logging.info("Converting %s to parquet.", self.input_name)
            data = self.read()
            self.write(data)


def call_conversion(in_name, out_name, mode):
    """
    Create object and call method to execute conversion. This is required so
    that multiprocessing can pickle a function, it cannot pickle a class.

    :param in_name: input file name in flat file, tsv or csv
    :param out_name: output file name in parquet
    :param mode: delimiter, comma or tab.
    """
    multiple = Conversion(in_name, out_name, mode)
    multiple.execute()


def parallelizer(input_directory, output_directory, mode):
    """
    Parallelizes this program to convert a whole directory of files.

    :param input_directory: input directory of flat files
    :param output_directory: name of directory to write to
    :param mode: delimiter in files, currently must be the same for whole
      directory
    """
    pool = Pool()
    job_tracker = {}
    for each in os.listdir(os.path.abspath(input_directory)):
        in_file = os.path.abspath(os.path.join(input_directory, each))
        out_name = os.path.join(os.path.abspath(output_directory),
                                os.path.basename(each) + '.parquet')
        job_tracker[out_name] = pool.apply_async(
            call_conversion, args=(in_file, out_name, mode))
    pool.close()
    pool.join()


def makedir_if_not_exist(directory):
    """
    Create a directory if it does not already exist.

    :param directory: (str) Directory name.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def command_line():
    """Collect and validate command line arguments."""
    class MyParser(argparse.ArgumentParser):
        """
        Override default behavior, print the whole help message for any CLI
        error.
        """

        def error(self, message):
            print('error: {}\n'.format(message), file=sys.stderr)
            self.print_help()
            sys.exit(2)

    parser = MyParser(description="Convert to Parquet")
    parser.add_argument(dest="input", help="Input filename",
                        type=str or unicode, nargs='?')
    parser.add_argument(dest="output", help="Output filename",
                        type=str or unicode, nargs='?')
    parser.add_argument('-id', '--input_directory', type=str or unicode,
                        help="Input directory of files to convert to parquet "
                             "in parallel. This overrides the positional "
                             "arguments for a single file.")
    parser.add_argument('-od', '--output_directory', type=str or unicode,
                        help="Specify output directory for the parquet files,"
                             " otherwise they are written to pwd.", default='.')
    file_format = parser.add_mutually_exclusive_group()
    file_format.add_argument("-tab", action="store_const", dest="file_type",
                             const="tab",
                             help="Set this argument if you are supplying a "
                             "tab-separated file. This is the default.")
    file_format.add_argument("-comma", action="store_const", dest="file_type",
                             const="comma",
                             help="set this argument if you are supplying a "
                             "comma-separated file.")
    parser.set_defaults(file_type='tab')
    arguments = parser.parse_args()
    if not arguments.input_directory:
        if arguments.input or arguments.output:
            if not arguments.input or not arguments.output:
                parser.error("You must specify both input file and output "
                             "filename")
        if not arguments.input and not arguments.output:
            parser.error(
                "You must use positional arguments to provide input and "
                "output filename or specify an input directory with "
                "-id/--input_directory")
    return arguments


if __name__ == "__main__":
    args = command_line()
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s '
                                                    '%(levelname)-8s %('
                                                    'message)s')
    if args.input and args.output:
        call_conversion(args.input, args.output, args.file_type)
    if args.input_directory:
        makedir_if_not_exist(os.path.abspath(args.output_directory))
        parallelizer(args.input_directory, args.output_directory,
                     args.file_type)
