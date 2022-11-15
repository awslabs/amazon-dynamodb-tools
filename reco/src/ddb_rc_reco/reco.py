import time
import queue
import logging
import re
import copy
import math
import os
import gzip
import multiprocessing
from numpy import median, average, std
from functools import lru_cache
from datetime import datetime, timedelta
from multiprocessing import Pool, JoinableQueue, Process, Queue
from ddb_rc_reco.config import test_file_loc, pricing, descriptions, ideal_sql
from ddb_rc_reco.config import plain_report_footer, rcu_regex, wcu_regex




'''
Setup
'''
logging.getLogger().setLevel(20)
logger = logging.getLogger('recommend')
log = logging.StreamHandler()
logger.addHandler(log)

region_re = dict()
for region_code, region_dict in pricing.items():
    region_re[region_code] = re.compile(region_dict['ut_region_match_filter'])

file_type_mode = int(0) #0 is CUR
operation_index = 1
usage_type_index = 2
start_time_index = 4
end_time_index = 5
usage_value_index = 6
cost_index = 7
li_description_index = 8

dt_format = '%m/%d/%y %H:%M:%S'
like_a_code = r'^[A-Z]{3}[0-9]-'
region_like = re.compile(like_a_code)
re_rcu_usage = re.compile(rcu_regex)
re_wcu_usage = re.compile(wcu_regex)

lang = 'en_us'
c_sym = '$'



'''
Display and outputs
'''

def generate_reco_tables(region_hours, wish):
    reco_table = dict()
    reco_table['_meta'] = dict()
    reco_table['_meta']['start_time'] = list()
    reco_table['_meta']['end_time'] = list()
    reco_table['_meta']['rc_term'] = wish['_meta']['rc_term']
    for region in region_hours:
        reco_table[region] = dict()
        for ut_regex, ut_dict in region_hours[region]['CommittedThroughput'].items():
            usage_type = ut_dict['usagetype']
            hours = ut_dict['hours']
            start_time = print_dt(ut_dict['hours'][0].dt)
            end_time = print_dt(ut_dict['hours'][-1].dt)
            if start_time not in reco_table['_meta']['start_time']:
                reco_table['_meta']['start_time'].append(start_time)
            if end_time not in reco_table['_meta']['end_time']:
                reco_table['_meta']['end_time'].append(end_time)
            reco_table[region][usage_type] = dict()
            reco_table[region][usage_type]['_meta'] = dict()
            od_unit_rate = pricing[region][usage_type]
            time_series = generate_tsv(hours)
            time_series_sorted = sorted(time_series)
            median_usg = median(time_series_sorted)
            min_usg = time_series_sorted[0]
            max_usg = time_series_sorted[-1]
            average_usg = average(time_series_sorted)
            std_usg = std(time_series_sorted)
            sum_usg = sum(time_series_sorted)
            reco_table[region][usage_type]['stats'] = {'min': min_usg, 'median': median_usg,
                'max': max_usg, 'average': average_usg, 'std_dev': std_usg, 'sum': sum_usg}
            logger.debug(wish)
            rc_unit = wish[region][usage_type]['sim_result']
            rc_term = wish[region][usage_type]['_meta']['rc_term']
            reco_table[region][usage_type]['_meta']['rc_term'] = rc_term
            rc_unit_rate = _get_rc_unit_rate(region, usage_type, rc_term)
            rc_unit_rate_no_amort = _get_rc_unit_hour(region, usage_type, rc_term) / 100.0
            rc_base = _get_rc_unit_base(region, usage_type, rc_term)
            od_unit_rate = pricing[region][usage_type]
            rc_base_total = (rc_unit / 100.0) * rc_base


            od_rate_total = _simulate_purchase(time_series, 0, rc_unit_rate, od_unit_rate)
            mixed_rate_total = _simulate_purchase(time_series, rc_unit, rc_unit_rate, od_unit_rate)
            no_amort_rate_total = _simulate_purchase(time_series, rc_unit, rc_unit_rate_no_amort, od_unit_rate)
            rc_percent_savings = 0.0
            if od_rate_total > 0:
                rc_percent_savings = round(((od_rate_total - mixed_rate_total)/ od_rate_total) * 100.0, 2)
            reco_table[region][usage_type]['recommendation'] = {'rc_unit': rc_unit,
                'rc_upfront': rc_base_total, 'od_only_rate': od_rate_total,
                'mixed_rate_total': mixed_rate_total, 'mixed_rate_excl_upfront': no_amort_rate_total,
                'percent_savings_over_od': rc_percent_savings}
    totals = dict() #Bill	Full OD, yearly, percent savings
    totals['od_only_rate'] = 0.0
    totals['mixed_rate_total'] = 0.0
    totals['rc_upfront'] = 0.0
    rt_keys = list()
    for key in reco_table.keys():
        if not key.find('_') == 0:
            rt_keys.append(key)

    for region in rt_keys:
        r_dict = reco_table[region]
        logger.debug("For region {} in reco_table".format(region))
        #filter totals
        ut_keys = list()
        for key in r_dict.keys():
            if not key.find('_') == 0:
                ut_keys.append(key)
        reco_table[region]['_totals'] = dict()
        #Full OD, Monthly, monthly amoritized, percent savings
        reco_table[region]['_totals']['od_only_rate'] = 0.0
        reco_table[region]['_totals']['mixed_rate_total'] = 0.0
        reco_table[region]['_totals']['mixed_rate_excl_upfront'] = 0.0
        reco_table[region]['_totals']['rc_upfront'] = 0.0
        for usage_type in ut_keys:
            logger.debug("For ut {} in reco_table".format(usage_type))
            reco_table[region]['_totals']['od_only_rate'] += reco_table[region][usage_type]['recommendation']['od_only_rate']
            reco_table[region]['_totals']['mixed_rate_total'] += reco_table[region][usage_type]['recommendation']['mixed_rate_total']
            reco_table[region]['_totals']['mixed_rate_excl_upfront'] += reco_table[region][usage_type]['recommendation']['mixed_rate_excl_upfront']
            reco_table[region]['_totals']['rc_upfront'] += reco_table[region][usage_type]['recommendation']['rc_upfront']
        if reco_table[region]['_totals']['od_only_rate'] > 0:
            reco_table[region]['_totals']['percent_savings_over_od']  = round(((reco_table[region]['_totals']['od_only_rate'] - reco_table[region]['_totals']['mixed_rate_total'])/ reco_table[region]['_totals']['od_only_rate']) * 100.0, 2)
        totals['od_only_rate'] += reco_table[region]['_totals']['od_only_rate']
        totals['mixed_rate_total'] += reco_table[region]['_totals']['mixed_rate_total']
        totals['rc_upfront'] += reco_table[region]['_totals']['rc_upfront']
    totals['percent_savings_over_od'] = round(((totals['od_only_rate'] - totals['mixed_rate_total'])/ totals['od_only_rate']) * 100.0, 2)
    totals['od_only_rate_yr'] = totals['od_only_rate'] * 12
    totals['mixed_rate_total_yr'] = totals['mixed_rate_total'] * 12
    def round_all_nums(a_dict):
        if isinstance(a_dict, dict):
            for k, v in a_dict.items():
                a_dict[k] = round_all_nums(v)
            return a_dict
        elif isinstance(a_dict, float):
            return round(a_dict, 2)
        else:
            return a_dict
    #wrap it in
    reco_table['_totals'] = totals
    round_all_nums(reco_table)
    return reco_table

def generate_tsv(hours):
    time_series = list()
    for hour in hours:
        time_series.append(hour.od_usage)
    return time_series

def output_csv(table_input):
    #TODO this introduces a bug. We should sort the times to find the max range, not select element 1
    line_start_time = table_input['_meta']['start_time'][0]
    line_end_time = table_input['_meta']['end_time'][0]
    region_keys = list()
    for key in table_input.keys():
        if not key.find('_') == 0:
            region_keys.append(key)
    headers = ['RegionCode', 'UsageType', 'StartTime', 'EndTime', 'Term',
        'RCUnits', 'UpfrontCost', 'FullODRate', 'EffectiveRate',
        'EffectiveRateExclUpfront', 'PercentSavingsOverOD', 'Minimum',
        'Median', 'Maximum', 'Average', 'StdDev', 'SumUsage' ]
    for k in range(len(headers)):
        headers[k] = '"' + headers[k] + '"'
    print(','.join(headers))
    for region in region_keys:
        region_code = descriptions[region][lang]['region']['code']
        line_region_code = region_code
        ut_keys = list()
        for key in table_input[region].keys():
            if not key.find('_') == 0:
                ut_keys.append(key)
        for usage_type in ut_keys:
            ut_dict = table_input[region][usage_type]
            line = list()
            line.append(line_region_code)
            line.append(usage_type)
            line.append(line_start_time)
            line.append(line_end_time)
            line.append(ut_dict['_meta']['rc_term'])
            line.append(ut_dict['recommendation']['rc_unit'])
            line.append(ut_dict['recommendation']['rc_upfront'])
            line.append(ut_dict['recommendation']['od_only_rate'])
            line.append(ut_dict['recommendation']['mixed_rate_total'])
            line.append(ut_dict['recommendation']['mixed_rate_excl_upfront'])
            line.append(ut_dict['recommendation']['percent_savings_over_od'])
            line.append(ut_dict['stats']['min'])
            line.append(ut_dict['stats']['median'])
            line.append(ut_dict['stats']['max'])
            line.append(ut_dict['stats']['average'])
            line.append(ut_dict['stats']['std_dev'])
            line.append(ut_dict['stats']['sum'])
            if not len(line) == len(headers):
                raise IndexError('Insufficienct number of columns for headers. {} found, {} columns expected'.format(len(line), len(headers)))
            for k in range(len(line)):
                line[k] = '"' + str(line[k]) + '"'
            print(','.join(line))


def output_table(table_input):
    region_keys = list()
    for key in table_input.keys():
        if not key.find('_') == 0:
            region_keys.append(key)

    def print_section_header(some_str, seperator='#'):
        str_len = len(some_str)
        hashes = 60
        padding = hashes - 2 - str_len
        padding = int(padding / 2) + 1
        if padding < 0:
            padding = 0
        print(''.join([seperator] * hashes))
        print(''.join([' '] * padding) + some_str)
        print(''.join([seperator] * hashes))
    #TODO this introduces a bug. We should sort the times to find the max range, not select element 1
    start_time = table_input['_meta']['start_time'][0]
    end_time = table_input['_meta']['end_time'][0]
    rc_term = table_input['_meta']['rc_term']
    days_in_report = (parse_dt(end_time)-parse_dt(start_time)).days
    print_section_header("DynamoDB Reserved Capacity Report\nThese recommendations are based on data from {} to {}. The time span represents {} days of data. RC term length for the report is {} year(s)\n{}\nGenerated on {}".format(start_time, end_time, days_in_report, rc_term, descriptions['banner'][lang]['warning_top']['text'],  print_dt(datetime.now()) ))
    if days_in_report < 28 or days_in_report > 31:
        dynamic_warning = "WARNING: Non-standard number of days detected in the report. The fields marked monthly in this report represent the total cost for the selected time period, not the true monthly cost.\nRe-run the report with data for only one month to receive accurate monthly cost estimates."
        print_section_header(dynamic_warning, "*")
    for region_code in region_keys:
        region_short_name = descriptions[region_code][lang]['region']['short_name']
        region_code_name = descriptions[region_code][lang]['region']['code']
        print_section_header("{} region - {}".format(region_short_name, region_code_name), '*')
        ut_keys = list()
        for key in table_input[region_code].keys():
            if not key.find('_') == 0:
                ut_keys.append(key)
        for usage_type in ut_keys:
            usage_type_caps = descriptions['cu'][lang][usage_type]['short_name']
            usage_type_name = descriptions['cu'][lang][usage_type]['name']
            usage_type_reserved = descriptions['cu'][lang][usage_type]['reserved']
            print('Capacity type: {}, Term: {} year(s)'.format(usage_type_reserved, table_input[region_code][usage_type]['_meta']['rc_term']))
            if 'recommendation' in table_input[region_code][usage_type]:
                reco = table_input[region_code][usage_type]['recommendation']
                print("\tReserved capacity to own (not necessarily buy): {:,} {}(s), which is bought as {} units of 100. Upfront cost: {}{:,.2f}".format(reco['rc_unit'], usage_type_caps, int(reco['rc_unit'] / 100), c_sym, reco['rc_upfront']))
                print("\tEffective monthly rate: {}{:,.2f}, monthly rate after first month: {}{:,.2f}\n\tMonthly savings over the public rate card: {}{:,.2f} ({}%)".format(c_sym, reco['mixed_rate_total'], c_sym, reco['mixed_rate_excl_upfront'], c_sym, reco['od_only_rate'] - reco['mixed_rate_total'], reco['percent_savings_over_od']))
            if 'stats' in table_input[region_code][usage_type]:
                stats = table_input[region_code][usage_type]['stats']
                if logger.level < 20: # don't confuse customers
                    print("\tmin: {:,} median: {:,} max: {:,} average: {:,} std_dev: {:,} sum: {:,}".format(stats['min'], stats['median'], stats['max'], stats['average'], stats['std_dev'], stats['sum']))
        if '_totals' in table_input[region_code]:
            tots = table_input[region_code]['_totals']
            print("Totals: Effective monthly rate: {}{:,.2f}, monthly rate after first month: {}{:,.2f}, Monthly savings over the public rate card: {}{:,.2f} ({}%)".format( c_sym, tots['mixed_rate_total'], c_sym, tots['mixed_rate_excl_upfront'], c_sym, tots['od_only_rate'] - tots['mixed_rate_total'], tots['percent_savings_over_od']))
    if '_totals' in table_input:
        tots = table_input['_totals']
        totals = "Totals: Effective monthly rate: {}{:,.2f}, Total savings over the public rate card for duration of term: {}{:,.2f} ({}%), Total upfront purchase cost: {}{:,.2f}".format(c_sym, tots['mixed_rate_total'], c_sym, tots['od_only_rate_yr'] - tots['mixed_rate_total_yr'] , tots['percent_savings_over_od'], c_sym, tots['rc_upfront'])
        print_section_header("Glossary", '*')
        print(descriptions['banner'][lang]['report_footer']['text'])
        print_section_header("End of report.\n{}".format(totals))



'''
Simulations
'''


#make a recommendation on committed throughput
def make_a_wish_single(region_hours, years):
    regions = region_hours.keys()
    is_my_command = dict();
    is_my_command['_meta'] = dict()
    is_my_command['_meta']['rc_term'] = years
    for region in regions:
        is_my_command[region] = ()
        is_my_command[region] = _make_a_wish(region, region_hours[region]['CommittedThroughput'] , years)
    return is_my_command

def make_a_wish(region_hours, years):
    regions = region_hours.keys()
    is_my_command = dict();
    is_my_command['_meta'] = dict()
    is_my_command['_meta']['rc_term'] = years
    # For each region, for each usage type, simulate the purchase
    cpu_count =  multiprocessing.cpu_count()
    waiting_regions = JoinableQueue()
    completed_regions = Queue()
    processes = []
    for region in regions:
        is_my_command[region] = ()
        logger.debug("Putting region {} into the waiting regions".format(region))
        waiting_regions.put([region, region_hours[region]['CommittedThroughput'] , years])

    for cpus in range(cpu_count):
        p = Process(target=wish_process, args=(waiting_regions, completed_regions, ))
        processes.append(p)
        logger.debug("Starting a new process to handle regions.")
        p.start()


    waiting_regions.join()
    for cpus in range(cpu_count):
        waiting_regions.put('STOP')
    time.sleep(2)
    for p in processes:
        p.terminate()
    while not completed_regions.empty():
        completed_region =  completed_regions.get()
        is_my_command[completed_region[0]] = completed_region[1]
    return is_my_command

def wish_process(waiting_regions, completed_regions):
    while True:
        try:
            logger.debug("Attempting to grab a region off the queue.")
            working_region = waiting_regions.get_nowait()
            if isinstance(working_region, str) and working_region == 'STOP':
                logger.debug("Received STOP. This thread is ending.")
                break
            logger.debug("Processing {}".format(working_region[0]))
            completed_region = _make_a_wish(working_region[0], working_region[1], working_region[2])
            waiting_regions.task_done()
        except queue.Empty:
            logger.debug("wish_process - Queue is empty. Sleeping 1s.")
            time.sleep(1)
        else:
            logger.debug("Putting {} into completed regions".format(working_region[0]))
            completed_regions.put([working_region[0], completed_region])




def _make_a_wish(region, committed_throughput, years):
    returnee = dict();
    for ut_regex, ut_dict in committed_throughput.items():
        usage_type = ut_dict['usagetype']
        hours = ut_dict['hours']
        od_unit_rate = pricing[region][usage_type]

        rc_unit_rate = None
        rc_term = None
        try:
            rc_term = years
            rc_unit_rate = _get_rc_unit_rate(region, usage_type, years)
        except KeyError as err:
            rc_term = 1
            logger.info("make_a_wish - requested{} year(s) RC term in {} but unable to find; falling back to {} year(s) term".format(years, region, rc_term))
            rc_unit_rate = _get_rc_unit_rate(region, usage_type, 1)

        time_series = generate_tsv(hours)

        sorted_sim_results = simulate_purchase(time_series, rc_unit_rate, od_unit_rate)
        returnee[usage_type] = dict()
        returnee[usage_type]['sim_result'] = sorted_sim_results[0][1]
        returnee[usage_type]['_meta'] = dict()
        returnee[usage_type]['_meta']['rc_term'] = rc_term
        logger.debug("make_a_wish - reco: {}: {} units: {}".format(region, usage_type, returnee[usage_type]))

    return returnee

# rc and od unit_rate are for a single unit-hour
def simulate_purchase(time_series, rc_unit_rate, od_unit_rate):
    sim_results = list()
    current_unit = 0
    upper_end = sorted(time_series)[-1]
    upper_end = 100 + int(math.ceil(upper_end/100.0)) * 100
    step = 100
    # TODO be less 'stupid' about how we seek the goal
    while current_unit <= upper_end:
        sim_total_cost = _simulate_purchase(time_series, current_unit, rc_unit_rate, od_unit_rate)
        sim_results.append((sim_total_cost, current_unit))
        current_unit += step
    #sorted(data, key=lambda tup: tup[1])
    return sorted(sim_results, key=lambda tup: tup[0])


def _simulate_purchase(time_series, unit, rc_unit_rate, od_unit_rate):
    running_total = 0.0
    for usage in time_series:
        running_total += unit * rc_unit_rate
        if usage  > unit:
            running_total += (usage - unit) * od_unit_rate
    return running_total


#assume 1 hr increments in range
def generate_hours(start, end):
    delta_one_hour = timedelta(hours=1)
    hours = []
    hours.append(hour(start))
    hours.append(hour(end))
    position = start
    if end - start > timedelta(hours=1):
        while position < end:
            position += delta_one_hour
            if position not in hours:
                hours.append(hour(position))

    hours_dedupe = list(set(hours))
    hours_sorted = sorted(hours_dedupe)
    logger.debug("generate_hours: generated {} hours. start {}, end {}".format(len(hours_sorted), start, end))
    if not len(hours_dedupe) == len(hours_sorted) == len(hours):
        raise ValueError('Duplicate hours created. Check start and end time.')

    return hours_sorted


def generate_hours_for_regions(start, end, regions):
    region_hours = dict()
    for region in regions:
        region_hour = dict()
        #TODO add regex for line_item_line_item_description to discover which unit type this is
        region_hour['CommittedThroughput'] = dict()
        #Can't buy RC for repl
        #region_hour['CommittedThroughput'][r'.*ReplWriteCapacityUnit.*'] = {'usagetype': 'rwcu', 'hours': generate_hours(start, end)}
        region_hour['CommittedThroughput'][rcu_regex] = {'usagetype': 'rcu', 'hours': generate_hours(start, end)}
        region_hour['CommittedThroughput'][wcu_regex] = {'usagetype': 'wcu', 'hours': generate_hours(start, end)}
        region_hours[region] = copy.copy(region_hour)
    return region_hours

'''
File handling
'''

def open_file_read(file_loc):
    fp, fn = os.path.splitext(file_loc)
    if fn.endswith('.gz'):
        return gzip.open(file_loc, mode='rt')
    else:
        return open(file_loc, 'r')


def process_csv(csv_iterator, region_hours):
    all_uts = list()
    for row in csv_iterator:
        if re_rcu_usage.search(row[usage_type_index]) or re_wcu_usage.search(row[usage_type_index]):
            region_code = get_region_for_usage_type(row[usage_type_index])
            if region_code:
                for operation, operation_dict in region_hours[region_code].items():
                    if row[operation_index] == operation:
                        for ut_regex, ut_dict in operation_dict.items():
                            if re.match(ut_regex, row[usage_type_index]):
                                if row[usage_type_index] not in all_uts:
                                    all_uts.append(row[usage_type_index])
                                usage_value = float(row[usage_value_index])
                                if float(row[cost_index]) >= 0.0: # exclude discounts, credits, and refunds
                                    hours = ut_dict['hours']
                                    start_time = parse_dt(row[start_time_index])
                                    end_time = parse_dt(row[end_time_index])

                                    if end_time in hours:
                                        end_range = hours.index(end_time)
                                    elif end_time > hours[-1].dt:
                                        end_range = len(hours)
                                    else:
                                        end_range = None
                                    if start_time in hours:
                                        start_range = hours.index(start_time)
                                    elif start_time < hours[0].dt:
                                        start_range = 0
                                    else:
                                        start_range = None
                                    if start_range is not None and end_range is not None:
                                        for index in range(start_range, end_range):
                                            if hours[index].dt >= start_time or hours[index].dt < end_time:
                                                hours[index].od_usage += usage_value # this wraps free tier, od, and reserved usage together. It also combines DLI_RT rows
    logger.debug("We are targetting the following UTs in this report: {}".format(', '.join(all_uts)))

def region_list(csv_iterator):
    # determine exhaustive list of regions and RCs
    found_regions = set()
    like_regions = set()
    for row in csv_iterator:
        found_region = None
        if re_rcu_usage.search(row[usage_type_index]) or re_wcu_usage.search(row[usage_type_index]):
            for region_code, region_find in region_re.items():
                if region_find.search(row[usage_type_index]):
                    found_region = region_code
                    break
            if found_region:
                found_regions.add(found_region)
            elif region_like.search(row[usage_type_index]):
                like_regions.add(region_like.search(row[usage_type_index]).group(0))
    if len(like_regions) > 0:
        raise RuntimeError('Unknown region(s) found. Tool not configured with pricing data: {}'.format(', '.join(list(like_regions))))
    return found_regions

# TODO don't match on this datapoint:
# "Amazon DynamoDB","CommittedThroughput","EU-WriteCapacityUnit-Hrs","","12/01/19 00:00:00","01/01/20 00:00:00","1.0","0.0","Tax for product code AmazonDynamoDB usage type EU-WriteCapacityUnit-Hrs operation CommittedThroughput"
def get_range_time(csv_iterator):
    next(csv_iterator) #trash header
    first_row_loaded = False
    for row in csv_iterator:
        if re_rcu_usage.search(row[usage_type_index]) or re_wcu_usage.search(row[usage_type_index]):
            row_start_time = parse_dt(row[start_time_index])
            row_end_time =  parse_dt(row[end_time_index])
            # Only use usage types with a price
            if row[li_description_index][0] == '$':
                if not first_row_loaded:
                    start_time =  row_start_time
                    end_time = row_end_time
                    first_row_loaded = True
                try:
                    if not (row_end_time - row_start_time) == timedelta(hours=1):
                        raise ValueError("Time start {} with end {} in the following row is not 1 hour wide (found {} delta).\n{}".format(print_dt(row_start_time), print_dt(row_end_time), (row_end_time, row_start_time), ", ".join(row)))
                except ValueError as err:
                    logger.error(err)
                    raise ValueError("CSV has read/write usage with granularity > 1 hour. Data detail is not sufficient. Report cannot continue.")
                if first_row_loaded:
                    if row_start_time < start_time:
                        start_time = row_start_time
                    if row_start_time > end_time:
                        end_time = row_start_time
    logger.debug("get_range_time start {} end {}".format(start_time, end_time))
    return start_time, end_time



def refresh_csv_index(file_type_mode_input):
    global file_type_mode, dt_format, operation_index, usage_type_index, start_time_index, end_time_index, usage_value_index, cost_index, li_description_index
    file_type_mode = file_type_mode_input
    if file_type_mode == 0:
        logger.info("Intepreting CUR data.")
        dt_format = '%m/%d/%y %H:%M:%S'
        operation_index = 1
        usage_type_index = 2
        start_time_index = 4
        end_time_index = 5
        usage_value_index = 6
        cost_index = 7
        li_description_index = 8
    elif file_type_mode == 1:
        logger.info("Intepreting DBR DLI data.")
        dt_format = '%Y-%m-%d %H:%M:%S'
        operation_index = 10 - 1
        usage_type_index = 9 - 1
        start_time_index = 14 - 1
        end_time_index =  15 - 1
        usage_value_index = 16 - 1
        cost_index = 20 - 1
        li_description_index = 13 -1
    elif file_type_mode == 2:
        #TODO add func tests for DLI_RT
        logger.info("Interpreting DBR dli_rt data")
        dt_format = '%Y-%m-%d %H:%M:%S'
        operation_index = 11 - 1
        usage_type_index = 10 - 1
        start_time_index = 15 - 1
        end_time_index =  16 - 1
        usage_value_index = 17 - 1
        cost_index = 21 - 1
        li_description_index = 14 -1
'''
Utility
'''
class hour(object):
    def __init__(self, dt):
        self.dt = dt
        self.rc_owned = 0.0
        self.od_usage = 0.0
    @property
    def od_usage(self):
        return self._od_usage
    @od_usage.setter
    def od_usage(self, value):
        self._od_usage = value
    @property
    def rc_owned(self):
        return self._rc_owned
    @rc_owned.setter
    def rc_owned(self, value):
        value = float(value)
        remainder = value % 100.0
        if remainder != 0.0:
            raise ValueError('RC can only be purchased in intervals of 100')
        self._rc_owned = value
    @property
    def dt(self):
        return self._dt
    @dt.setter
    def dt(self, dt):
        if  isinstance(dt, datetime):
            self._dt = dt
        else:
            raise ValueError('Datetime obj expected for comparison. type {} received'.format(type(dt)))

    def __lt__(self, cmp):
        return self.dt < cmp.dt
    def __gt__(self, cmp):
        return self.dt > cmp.dt
    def __eq__(self, cmp):
        if isinstance(cmp, hour):
            return self.dt == cmp.dt
        elif isinstance(cmp, datetime): # TODO why is this needed? generate_hours throws error w.o. this
            return self.dt == cmp
    def __hash__(self):
        return hash(self.dt)
    def __str__(self):
        return "od usage: {}, dt {}".format(self.od_usage, str(self.dt))



def parse_dt(dt_str):
     return datetime.strptime(dt_str, dt_format)
def print_dt(dt):
     return dt.strftime(dt_format)


@lru_cache(maxsize=32)
def _get_rc_unit_rate(region, usage_type, rc_term):
    rc_unit_rate =  _get_rc_unit_base(region, usage_type, rc_term) / float(365.0 * 24.0 * float(rc_term))
    rc_unit_rate += _get_rc_unit_hour(region, usage_type, rc_term)
    return rc_unit_rate / 100.0
@lru_cache(maxsize=32)
def _get_rc_unit_base(region, usage_type, rc_term):
    try:
        return float(pricing[region]['rc' + str(rc_term)][usage_type]['base'])
    except KeyError as err:
        if str(err) == "'rc3'":
            logger.debug('3 year RC term not found for this region {}.'.format(region))
        raise err
@lru_cache(maxsize=32)
def _get_rc_unit_hour(region, usage_type, rc_term):
    try:
        return float(pricing[region]['rc' + str(rc_term)][usage_type]['hour'])
    except KeyError as err:
        if str(err) == "'rc3'":
            logger.debug('3 year RC term not found for this region {}.'.format(region))
        raise err





def get_region_for_usage_type(usage_type):
    for region_code, region_regex in region_re.items():
        if region_regex.search(usage_type):
            return region_code
    if region_like.search(usage_type):
        raise RuntimeException('UsageType {} cannot be backtracked to source region'.format(usage_type))
