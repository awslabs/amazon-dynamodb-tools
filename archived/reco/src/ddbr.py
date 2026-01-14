import os
import sys
import csv
import argparse
import logging
from zipfile import ZipFile
from tempfile import NamedTemporaryFile
from datetime import timedelta

from ddb_rc_reco.reco import generate_hours_for_regions, make_a_wish
from ddb_rc_reco.reco import refresh_csv_index, process_csv, get_range_time, dt_format
from ddb_rc_reco.reco import parse_dt, print_dt, open_file_read, region_list
from ddb_rc_reco.reco import generate_reco_tables, output_table, output_csv
from ddb_rc_reco.config import ideal_sql, version
'''

DDBR CLI for DynamoDB RC recommendations

'''

logging.getLogger().setLevel(20)
logger = logging.getLogger('ddbr')
log = logging.StreamHandler()
logger.addHandler(log)




def main():
    arg_modes = ['reco']
    main_parser = argparse.ArgumentParser(description='Tool to simulate RC purchases')
    sub_parser = main_parser.add_subparsers(help=None)

    reco_parser = sub_parser.add_parser(arg_modes[0], help='Generate recommendation using AWS Usage / CUR data.')
    output_options = ['plain', 'csv', 'dict', 'all']
    rc_terms = ['1', '3', 'all']
    file_data_type = ['cur', 'dli', 'dli_rt']
    reco_parser.add_argument('--athena-sql', action='store_true', help='generate Athena SQL and exit')
    reco_parser.add_argument('--debug', action='store_true', help='Turn up log level')
    reco_parser.add_argument('--file-name', type=str, help='File name or path to file where usage data resides.')
    reco_parser.add_argument('--term', choices=rc_terms, help='RC term length to consider.')
    reco_parser.add_argument('--file-type', choices=file_data_type, help='CUR Athena query (cur) or DBR file (dli*). Detailed line items (dli). DLI with resources and tags (dli_rt)', default=file_data_type[0])
    reco_parser.add_argument('--output', type=str, help='Format of text to be displayed', choices=output_options)
    reco_parser.add_argument('--start-time', help="Start time with leading zeroes in format --start-time \"{}\"".format(dt_format.replace('%', '%%')), type=parse_dt)
    reco_parser.add_argument('--end-time', help="End time with leading zeroes in format --end-time \"{}\"".format(dt_format.replace('%', '%%')), type=parse_dt)
    reco_parser.add_argument('--package', help="Should output be ZIP'd into a user-deliverable format. Provide the package ZIP suffix", type=str)
    reco_parser.add_argument('--version', action='store_true', help='Print version and exit.')
    # TODO reco_parser.add_argument('--region', type=str, choices=pricing.keys(), help='Airport code for region to process')

    args = main_parser.parse_args()
    if args.debug is True:
        logging.getLogger().setLevel(10)
    if args.version is True:
        print("reco v{}".format(version))
    elif args.athena_sql is True:
        print(ideal_sql)
    elif args.file_name:
        csv_loc = args.file_name
        terms = [args.term]
        if any(term in terms for term in rc_terms):
            if rc_terms[2] in terms:
                terms = [rc_terms[0], rc_terms[1]]
        else:
            logger.info("Defaulting to 1 year RC term.")
            terms = [rc_terms[0]] # default to 1 yr
        start_time = None
        end_time = None
        found_regions = None
        region_hours = None

        refresh_csv_index(file_data_type.index(args.file_type))

        try:
            with open_file_read(csv_loc) as csvfile:
                row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                try:
                    start_time, end_time = get_range_time(row_reader)
                    if args.start_time:
                        if args.start_time < start_time:
                            raise RuntimeError("The start time of {} is not within the dataset start time of {}".format(args.start_time, start_time))
                        start_time = args.start_time
                    if args.end_time:
                        if args.end_time > end_time:
                            raise RuntimeError("The end time of {} is not within the dataset end time of {}".format(args.end_time, end_time))
                        end_time = args.end_time
                    logger.info("Recommendation will be generated between {} and {} based on source data.".format(print_dt(start_time), print_dt(end_time)))
                except UnboundLocalError as err:
                    logger.error("The input data file did not match the format we expected. Verify the file-type, and make sure the data has CapacityUnit-Hrs usage types with a cost.")
                    raise err
            days_at_target_time = (end_time + timedelta(hours=1) - start_time).days
            if days_at_target_time < 28 or days_at_target_time > 31:
                logger.warning("WARNING: The selected start and end times in file have greater or fewer days than a normal month. As a result, the 'monthly' summaries in the output will not reflect the true monthly cost.")
            with open_file_read(csv_loc) as csvfile:
                row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                found_regions = region_list(row_reader)
                region_hours = generate_hours_for_regions(start_time, end_time, found_regions)
                logger.info("Recommendation will be generated for {} region(s)".format(len(region_hours)))
            with open_file_read(csv_loc) as csvfile:
                csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_iterator) # dump header
                logger.info("Loading CSV into memory. Please wait.")
                process_csv(csv_iterator, region_hours)
            logger.info("Generating recommendations.")
            should_package = args.package is not None
            ntf_package = dict()
            for term in terms:
                wish = make_a_wish(region_hours, term)
                reco_table = generate_reco_tables(region_hours, wish)
                def shunt_to_file(file_name, output_method, *input):
                    stdout = sys.stdout
                    if sys.platform == 'win32':
                        ntf = NamedTemporaryFile(delete=False, mode='w')
                    else:
                        ntf = NamedTemporaryFile(delete=True, mode='w')
                    sys.stdout = ntf
                    #print("HELLO WORLD")
                    output_method(*input)
                    sys.stdout = stdout
                    ntf.flush()
                    ntf_package[file_name] = ntf
                if args.output in [output_options[1], output_options[3]]:
                    output_csv(reco_table)
                    if should_package:
                        shunt_to_file('rc-{}-{}-year.csv'.format(args.package, term), output_csv, reco_table)
                if args.output in [output_options[2], output_options[3]]:
                    print(reco_table)
                    if should_package:
                        #Do not output python dictionary to file
                        pass
                if args.output in [output_options[0], output_options[3]]:
                    output_table(reco_table)
                    if should_package:
                        shunt_to_file('rc-{}-{}-year.txt'.format(args.package, term), output_table, reco_table)
            logger.debug(should_package)
            if should_package and len(ntf_package):
                with ZipFile('recommendations-{}.zip'.format(args.package), 'w') as reco_zip:
                    folder_fmt = '%Y-%m-%d'
                    folder_prefix = "{} to {}".format(start_time.strftime(folder_fmt), end_time.strftime(folder_fmt))
                    for file_name, ntf in ntf_package.items():
                        output_file_name = os.path.join(args.package, folder_prefix, file_name)
                        logger.debug("Writing file {} to zip at location '{}'".format(ntf.name, output_file_name))
                        reco_zip.write(ntf.name, arcname=output_file_name)
                        ntf.close()
            if sys.platform == 'win32':
                os.unlink(ntf.name)

        except FileNotFoundError as err:
            logger.error("error: rec'd filename '{}' but this file name is not found in our base path '{}'.".format(args.file_name, os.getcwd()))
    else:
        raise RuntimeException('Missing valid arguments. Run -h for more information on options.')
if __name__ == '__main__':
    main()
