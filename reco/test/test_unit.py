import copy
import sys
import re
import traceback
from unittest import TestCase
from io import StringIO
from ddb_rc_reco.config import test_file_loc, rcu_regex, wcu_regex
from ddb_rc_reco.reco import hour, get_range_time, parse_dt, generate_hours, region_list, generate_hours_for_regions
from ddb_rc_reco.reco import get_region_for_usage_type, process_csv, generate_tsv, _simulate_purchase, pricing, _get_rc_unit_rate
from ddb_rc_reco.reco import simulate_purchase, output_table, output_csv, open_file_read, refresh_csv_index

class test_reco(TestCase):
    test_csv_apn1 = 'test/APN1.csv.gz'
    test_csv_apn1_reads_only = 'test/APN1_reads.csv.gz'
    test_csv_apn1_daily_granularity = 'test/APN1_daily_granularity.csv.gz'
    test_csv_usw2_rwcu_wcu = 'test/USW2_rWCU_WCU.gz'

    def setUp(self):
        refresh_csv_index(0)

    def test_open_file_read(self):
        with self.assertRaises(FileNotFoundError):
            try:
                open_file_read('some.gz')
            except FileNotFoundError as err:
                self.assertIn('gzip.py', traceback.format_exc())
                raise err
        with self.assertRaises(FileNotFoundError):
            try:
                open_file_read('some.csv')
            except FileNotFoundError as err:
                self.assertNotIn('gzip.py', traceback.format_exc())
                raise err

    def test_output_csv(self):
        local_table_output = copy.copy(reco_table_output)
        orig_sysout = sys.stdout
        sys.stdout = my_stdout = StringIO()
        output_csv(local_table_output)
        sys.stdout = orig_sysout

    def test_output_table(self):
        local_table_output = copy.copy(reco_table_output)
        orig_sysout = sys.stdout
        sys.stdout = my_stdout = StringIO()
        output_table(local_table_output)
        sys.stdout = orig_sysout

    def test_hour(self):
        sample_dt = parse_dt('05/01/19 00:00:00')
        sample_hour = hour(sample_dt)
        sample_hour.od_usage = 100.0
        sample_hour.rc_owned = 400.0

        sample_hour = hour(sample_dt)
        sample_hour.od_usage = 100.0
        sample_hour.rc_owned = 0.0

        with self.assertRaises(ValueError):
            sample_hour = hour(sample_dt)
            sample_hour.od_usage = 100.0
            sample_hour.rc_owned = 10.0
        with self.assertRaises(ValueError):
            hour(None)

    def test_get_range_time(self):
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        csv_loc = test_file_loc
        import csv
        with open_file_read(csv_loc) as csvfile:
            row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            start, end = get_range_time(row_reader)
            self.assertEqual(expected_end, end)
            self.assertEqual(expected_start, start)
        csv_loc = self.test_csv_apn1_daily_granularity
        import csv
        with open_file_read(csv_loc) as csvfile:
            row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            with self.assertRaises(ValueError):
                get_range_time(row_reader)

    def test_generate_hours(self):
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        hours = generate_hours(expected_start, expected_end)
        self.assertEqual(744, len(hours))

        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/01/19 01:00:00')
        hours = generate_hours(expected_start, expected_end)
        self.assertEqual(2, len(hours))

        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/01/19 02:00:00')
        hours = generate_hours(expected_start, expected_end)
        self.assertEqual(3, len(hours))

    def test_region_list(self):
        csv_loc = test_file_loc
        import csv
        with open_file_read(csv_loc) as csvfile:
            row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            found_regions = region_list(row_reader)
            self.assertEqual(len(found_regions), 15)
            self.assertTrue('IAD' in found_regions)
        with self.assertRaises(RuntimeError):
            unknown_region = [['Amazon DynamoDB', 'CommittedThroughput', 'XYZ3-ReadCapacityUnit-Hrs', '05/01/19 00:00:00','05/02/19 00:00:00','0.0','0.0','sample']]
            region_list(unknown_region)

    def test_generate_hours_for_regions(self):
        regions = ['GRU', 'NRT', 'HKG', 'FRA', 'YYZ', 'BOM', 'ICN', 'ARN', 'SYD', 'LHR', 'IAD', 'CMH', 'SFO', 'CDG', 'PDX', 'KIX', 'PDT', 'SIN']
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        region_hours = generate_hours_for_regions(expected_start, expected_end, regions)
        self.assertEqual(len(regions), len(region_hours))

    def test_get_region_for_usage_type(self):
        ut_good = 'APN1-ReadCapacityUnit-Hrs'
        ut_bad = 'XYZ1-ReadCapacityUnit-Hrs'
        self.assertEqual(get_region_for_usage_type(ut_good), 'NRT')
        with self.assertRaises(Exception):
            get_region_for_usage_type(ut_bad)

    def test_generate_tsv(self):
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        hours = generate_hours(expected_start, expected_end)
        time_series = generate_tsv(hours)
        self.assertEqual(744, len(time_series))

        regions = ['NRT']
        region_hours = generate_hours_for_regions(expected_start, expected_end, regions)
        csv_loc = self.test_csv_apn1_reads_only
        import csv
        with open_file_read(csv_loc) as csvfile:
            csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(csv_iterator) # dump header
            process_csv(csv_iterator, region_hours)
        time_series = generate_tsv(region_hours['NRT']['CommittedThroughput'][rcu_regex]['hours'])
        self.assertEqual(744, len(time_series))

    def test_get_rc_unit_rate(self):
        rate = _get_rc_unit_rate('NRT', 'rcu', 1)
        self.assertEqual(round(rate, 14), 0.00006804109589)
        rate = _get_rc_unit_rate('NRT', 'rcu', 3)
        self.assertEqual(round(rate, 14), 0.00003360121766)

    def test_simulate_purchase(self):
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        regions = ['NRT']
        region_hours = generate_hours_for_regions(expected_start, expected_end, regions)
        csv_loc = self.test_csv_apn1_reads_only
        import csv
        with open_file_read(csv_loc) as csvfile:
            csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(csv_iterator) # dump header
            process_csv(csv_iterator, region_hours)
        time_series = generate_tsv(region_hours['NRT']['CommittedThroughput'][rcu_regex]['hours'])
        running_total = 0.0
        for datapoint in time_series:
            running_total += datapoint
        rc_unit_rate = _get_rc_unit_rate('NRT', 'rcu', 1)
        od_unit_rate = pricing['NRT']['rcu']
        unit = 0
        sim = _simulate_purchase(time_series, unit, rc_unit_rate, od_unit_rate)
        self.assertEqual(running_total, 36792145)
        self.assertEqual(od_unit_rate, 0.0001484)
        self.assertEqual(round(sim, 6), round(running_total * od_unit_rate, 6))

        sims = simulate_purchase(time_series, rc_unit_rate, od_unit_rate)
        soln_tup = sims[0]
        self.assertEqual(49800, soln_tup[1])

    def test_process_csv(self):
        expected_start = parse_dt('05/01/19 00:00:00')
        expected_end = parse_dt('05/31/19 23:00:00')
        regions = ['NRT']
        region_hours = generate_hours_for_regions(expected_start, expected_end, regions)
        csv_loc = self.test_csv_apn1
        import csv
        with open_file_read(csv_loc) as csvfile:
            csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(csv_iterator) # dump header
            process_csv(csv_iterator, region_hours)
        self.assertEqual(len(region_hours['NRT']['CommittedThroughput'][rcu_regex]['hours']), 744)
        self.assertEqual(len(region_hours['NRT']['CommittedThroughput'][wcu_regex]['hours']), 744)
        self.assertEqual(region_hours['NRT']['CommittedThroughput'][rcu_regex]['hours'][0].od_usage, 33716.0)
        self.assertEqual(region_hours['NRT']['CommittedThroughput'][rcu_regex]['hours'][-1].od_usage, 41551.0)
        self.assertEqual(region_hours['NRT']['CommittedThroughput'][wcu_regex]['hours'][0].od_usage, 13575.0)
        self.assertEqual(region_hours['NRT']['CommittedThroughput'][wcu_regex]['hours'][-1].od_usage, 15539.0)

    def test_regex(self):
        def eval_regex(regex, passers, failers):
            print("Evaluating regex: {}".format(regex))
            for ut in passers:
                try:
                    self.assertTrue(isinstance(re.match(regex, ut), re.Match))
                except AssertionError as err:
                    print("FAIL: UT {} DID NOT match :-(".format(ut))
                    raise err
            for ut in failers:
                try:
                    self.assertTrue(not isinstance(re.match(regex, ut), re.Match))
                except AssertionError as err:
                    print("FAIL: UT {} matched :-(".format(ut))
                    raise err
        always_fail = ['APN1-HeavyUsage:dynamodb.write', 'APN1-HeavyUsage:dynamodb.read', 'USW2-DataTransfer-Out-Bytes']
        regex = wcu_regex
        passers = ['USW2-WriteCapacityUnit-Hrs', 'WriteCapacityUnit-Hrs']
        failers = ['USW2-ReplWriteCapacityUnit-Hrs', 'ReplWriteCapacityUnit-Hrs']
        eval_regex(regex, passers, failers + always_fail)
        regex = rcu_regex
        passers = ['USW2-ReadCapacityUnit-Hrs', 'ReadCapacityUnit-Hrs']
        failers = ['USW2-ReplWriteCapacityUnit-Hrs', 'USW2-WriteCapacityUnit-Hrs']
        eval_regex(regex, passers, failers + always_fail)
    def test_process_csv_gt(self):
        expected_start = parse_dt('11/01/19 00:00:00')
        expected_end = parse_dt('11/25/19 15:00:00')
        regions = ['PDX']
        region_hours = generate_hours_for_regions(expected_start, expected_end, regions)
        refresh_csv_index(1)
        csv_loc = self.test_csv_usw2_rwcu_wcu
        import csv
        with open_file_read(csv_loc) as csvfile:
            csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
            process_csv(csv_iterator, region_hours)
        # rWCU != WCU :-/
        self.assertEqual(region_hours['PDX']['CommittedThroughput'][wcu_regex]['hours'][0].od_usage, 3177.0)
        self.assertEqual(region_hours['PDX']['CommittedThroughput'][wcu_regex]['hours'][-1].od_usage, 3095.0)

#python3 reco.py reco --term 1 --file-name b8f1d493-9aa2-4e5b-a5e8-121b7cfa131e.csv --output dict
reco_table_output = {'_meta': {'start_time': ['05/01/19 00:00:00'], 'end_time': ['05/31/19 23:00:00'], 'rc_term': '1'}, 'YUL': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 2909.0, 'median': 6387.0, 'max': 17113.0, 'average': 7027.19, 'std_dev': 2210.47, 'sum': 5228230.0}, 'recommendation': {'rc_unit': 6500, 'rc_upfront': 2145.0, 'od_only_rate': 747.64, 'mixed_rate_total': 425.82, 'mixed_rate_excl_upfront': 243.64, 'percent_savings_over_od': 43.04}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 1631.0, 'median': 2418.0, 'max': 5318.0, 'average': 2508.56, 'std_dev': 576.14, 'sum': 1866369.0}, 'recommendation': {'rc_unit': 2500, 'rc_upfront': 4125.0, 'od_only_rate': 1334.45, 'mixed_rate_total': 730.48, 'mixed_rate_excl_upfront': 380.14, 'percent_savings_over_od': 45.26}}, '_totals': {'od_only_rate': 2082.09, 'mixed_rate_total': 1156.3, 'mixed_rate_excl_upfront': 623.78, 'rc_upfront': 6270.0, 'percent_savings_over_od': 44.46}}, 'SYD': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 7317.0, 'median': 19289.0, 'max': 43494.0, 'average': 20518.04, 'std_dev': 7493.72, 'sum': 15265422.0}, 'recommendation': {'rc_unit': 20000, 'rc_upfront': 6840.0, 'od_only_rate': 2259.28, 'mixed_rate_total': 1369.06, 'mixed_rate_excl_upfront': 788.13, 'percent_savings_over_od': 39.4}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 3319.0, 'median': 7934.5, 'max': 17504.0, 'average': 8455.47, 'std_dev': 2741.35, 'sum': 6290868.0}, 'recommendation': {'rc_unit': 8200, 'rc_upfront': 14022.0, 'od_only_rate': 4655.24, 'mixed_rate_total': 2741.43, 'mixed_rate_excl_upfront': 1550.52, 'percent_savings_over_od': 41.11}}, '_totals': {'od_only_rate': 6914.52, 'mixed_rate_total': 4110.48, 'mixed_rate_excl_upfront': 2338.64, 'rc_upfront': 20862.0, 'percent_savings_over_od': 40.55}}, 'DUB': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 45860.0, 'median': 203502.0, 'max': 530958.0, 'average': 232170.29, 'std_dev': 103175.11, 'sum': 172734698.0}, 'recommendation': {'rc_unit': 215300, 'rc_upfront': 72986.7, 'od_only_rate': 25392.0, 'mixed_rate_total': 16476.64, 'mixed_rate_excl_upfront': 10277.77, 'percent_savings_over_od': 35.11}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 27264.0, 'median': 73671.0, 'max': 177397.0, 'average': 77328.77, 'std_dev': 30042.1, 'sum': 57532605.0}, 'recommendation': {'rc_unit': 76800, 'rc_upfront': 130176.0, 'od_only_rate': 42286.46, 'mixed_rate_total': 26177.32, 'mixed_rate_excl_upfront': 15121.28, 'percent_savings_over_od': 38.1}}, '_totals': {'od_only_rate': 67678.47, 'mixed_rate_total': 42653.96, 'mixed_rate_excl_upfront': 25399.05, 'rc_upfront': 203162.7, 'percent_savings_over_od': 36.98}}, 'LHR': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 7168.0, 'median': 12425.5, 'max': 30340.0, 'average': 13235.73, 'std_dev': 3304.89, 'sum': 9847380.0}, 'recommendation': {'rc_unit': 12700, 'rc_upfront': 4521.2, 'od_only_rate': 1520.44, 'mixed_rate_total': 849.96, 'mixed_rate_excl_upfront': 465.97, 'percent_savings_over_od': 44.1}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 5060.0, 'median': 7169.0, 'max': 12335.0, 'average': 7340.27, 'std_dev': 1287.77, 'sum': 5461161.0}, 'recommendation': {'rc_unit': 7300, 'rc_upfront': 12994.0, 'od_only_rate': 4216.02, 'mixed_rate_total': 2228.87, 'mixed_rate_excl_upfront': 1125.27, 'percent_savings_over_od': 47.13}}, '_totals': {'od_only_rate': 5736.45, 'mixed_rate_total': 3078.83, 'mixed_rate_excl_upfront': 1591.24, 'rc_upfront': 17515.2, 'percent_savings_over_od': 46.33}}, 'CDG': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 1998.0, 'median': 28376.5, 'max': 46826.0, 'average': 26284.81, 'std_dev': 10220.7, 'sum': 19555895.0}, 'recommendation': {'rc_unit': 29200, 'rc_upfront': 10395.2, 'od_only_rate': 3019.43, 'mixed_rate_total': 1880.26, 'mixed_rate_excl_upfront': 997.38, 'percent_savings_over_od': 37.73}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 1258.0, 'median': 8858.5, 'max': 28116.0, 'average': 10912.09, 'std_dev': 6894.89, 'sum': 8118593.0}, 'recommendation': {'rc_unit': 9600, 'rc_upfront': 17088.0, 'od_only_rate': 6267.55, 'mixed_rate_total': 4514.89, 'mixed_rate_excl_upfront': 3063.58, 'percent_savings_over_od': 27.96}}, '_totals': {'od_only_rate': 9286.98, 'mixed_rate_total': 6395.16, 'mixed_rate_excl_upfront': 4060.97, 'rc_upfront': 27483.2, 'percent_savings_over_od': 31.14}}, 'BOM': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 9917.0, 'median': 23571.5, 'max': 68122.0, 'average': 26632.37, 'std_dev': 11371.25, 'sum': 19814483.0}, 'recommendation': {'rc_unit': 24600, 'rc_upfront': 8413.2, 'od_only_rate': 2932.54, 'mixed_rate_total': 1807.0, 'mixed_rate_excl_upfront': 1092.45, 'percent_savings_over_od': 38.38}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 6767.0, 'median': 11500.5, 'max': 27351.0, 'average': 12848.88, 'std_dev': 4528.6, 'sum': 9559569.0}, 'recommendation': {'rc_unit': 12000, 'rc_upfront': 20520.0, 'od_only_rate': 7074.08, 'mixed_rate_total': 4298.51, 'mixed_rate_excl_upfront': 2555.71, 'percent_savings_over_od': 39.24}}, '_totals': {'od_only_rate': 10006.62, 'mixed_rate_total': 6105.5, 'mixed_rate_excl_upfront': 3648.16, 'rc_upfront': 28933.2, 'percent_savings_over_od': 38.99}}, 'SIN': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 70503.0, 'median': 233282.5, 'max': 594768.0, 'average': 251836.68, 'std_dev': 110075.7, 'sum': 187366493.0}, 'recommendation': {'rc_unit': 255200, 'rc_upfront': 87278.4, 'od_only_rate': 27730.24, 'mixed_rate_total': 17806.03, 'mixed_rate_excl_upfront': 10393.34, 'percent_savings_over_od': 35.79}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 28984.0, 'median': 77343.5, 'max': 148395.0, 'average': 79679.29, 'std_dev': 26766.33, 'sum': 59281394.0}, 'recommendation': {'rc_unit': 81400, 'rc_upfront': 139194.0, 'od_only_rate': 43868.23, 'mixed_rate_total': 26558.94, 'mixed_rate_excl_upfront': 14736.98, 'percent_savings_over_od': 39.46}}, '_totals': {'od_only_rate': 71598.47, 'mixed_rate_total': 44364.97, 'mixed_rate_excl_upfront': 25130.33, 'rc_upfront': 226472.4, 'percent_savings_over_od': 38.04}}, 'PDT': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 53627.0, 'median': 139803.0, 'max': 222448.0, 'average': 139754.57, 'std_dev': 41182.95, 'sum': 103977399.0}, 'recommendation': {'rc_unit': 144100, 'rc_upfront': 51876.0, 'od_only_rate': 16220.47, 'mixed_rate_total': 9511.1, 'mixed_rate_excl_upfront': 5105.19, 'percent_savings_over_od': 41.36}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 29220.0, 'median': 80973.0, 'max': 127724.0, 'average': 76355.18, 'std_dev': 22381.45, 'sum': 56808251.0}, 'recommendation': {'rc_unit': 83700, 'rc_upfront': 150660.0, 'od_only_rate': 44310.44, 'mixed_rate_total': 23663.91, 'mixed_rate_excl_upfront': 10868.13, 'percent_savings_over_od': 46.6}}, '_totals': {'od_only_rate': 60530.91, 'mixed_rate_total': 33175.0, 'mixed_rate_excl_upfront': 15973.32, 'rc_upfront': 202536.0, 'percent_savings_over_od': 45.19}}, 'CMH': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 1.0, 'median': 5.0, 'max': 5.0, 'average': 4.99, 'std_dev': 0.15, 'sum': 3716.0}, 'recommendation': {'rc_unit': 0, 'rc_upfront': 0.0, 'od_only_rate': 0.48, 'mixed_rate_total': 0.48, 'mixed_rate_excl_upfront': 0.48, 'percent_savings_over_od': 0.0}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 1.0, 'median': 5.0, 'max': 5.0, 'average': 4.99, 'std_dev': 0.15, 'sum': 3716.0}, 'recommendation': {'rc_unit': 0, 'rc_upfront': 0.0, 'od_only_rate': 2.42, 'mixed_rate_total': 2.42, 'mixed_rate_excl_upfront': 2.42, 'percent_savings_over_od': 0.0}}, '_totals': {'od_only_rate': 2.9, 'mixed_rate_total': 2.9, 'mixed_rate_excl_upfront': 2.9, 'rc_upfront': 0.0, 'percent_savings_over_od': 0.0}}, 'FRA': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 41085.0, 'median': 133906.5, 'max': 462209.0, 'average': 157015.19, 'std_dev': 79081.41, 'sum': 116819299.0}, 'recommendation': {'rc_unit': 143500, 'rc_upfront': 52521.0, 'od_only_rate': 18527.54, 'mixed_rate_total': 12204.74, 'mixed_rate_excl_upfront': 7744.05, 'percent_savings_over_od': 34.13}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 19364.0, 'median': 52005.0, 'max': 234309.0, 'average': 60678.49, 'std_dev': 31757.24, 'sum': 45144793.0}, 'recommendation': {'rc_unit': 54800, 'rc_upfront': 100284.0, 'od_only_rate': 35799.82, 'mixed_rate_total': 22591.87, 'mixed_rate_excl_upfront': 14074.6, 'percent_savings_over_od': 36.89}}, '_totals': {'od_only_rate': 54327.36, 'mixed_rate_total': 34796.61, 'mixed_rate_excl_upfront': 21818.65, 'rc_upfront': 152805.0, 'percent_savings_over_od': 35.95}}, 'SFO': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 102923.0, 'median': 224438.5, 'max': 479132.0, 'average': 240237.78, 'std_dev': 54170.22, 'sum': 178736905.0}, 'recommendation': {'rc_unit': 229100, 'rc_upfront': 76977.6, 'od_only_rate': 25916.85, 'mixed_rate_total': 14141.87, 'mixed_rate_excl_upfront': 7604.05, 'percent_savings_over_od': 45.43}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 25723.0, 'median': 144680.0, 'max': 172066.0, 'average': 140844.59, 'std_dev': 21635.34, 'sum': 104788378.0}, 'recommendation': {'rc_unit': 146600, 'rc_upfront': 246288.0, 'od_only_rate': 75971.57, 'mixed_rate_total': 38827.52, 'mixed_rate_excl_upfront': 17909.91, 'percent_savings_over_od': 48.89}}, '_totals': {'od_only_rate': 101888.43, 'mixed_rate_total': 52969.39, 'mixed_rate_excl_upfront': 25513.96, 'rc_upfront': 323265.6, 'percent_savings_over_od': 48.01}}, 'IAD': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 126841.0, 'median': 631515.0, 'max': 1158671.0, 'average': 619332.25, 'std_dev': 198891.69, 'sum': 460783197.0}, 'recommendation': {'rc_unit': 647700, 'rc_upfront': 194310.0, 'od_only_rate': 59901.82, 'mixed_rate_total': 35120.03, 'mixed_rate_excl_upfront': 18616.99, 'percent_savings_over_od': 41.37}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 71774.0, 'median': 226434.0, 'max': 510843.0, 'average': 244582.02, 'std_dev': 76654.91, 'sum': 181969022.0}, 'recommendation': {'rc_unit': 239200, 'rc_upfront': 358800.0, 'od_only_rate': 118279.86, 'mixed_rate_total': 69997.81, 'mixed_rate_excl_upfront': 39524.38, 'percent_savings_over_od': 40.82}}, '_totals': {'od_only_rate': 178181.68, 'mixed_rate_total': 105117.84, 'mixed_rate_excl_upfront': 58141.38, 'rc_upfront': 553110.0, 'percent_savings_over_od': 41.01}}, 'NRT': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 8843.0, 'median': 48226.0, 'max': 97725.0, 'average': 49451.81, 'std_dev': 15766.74, 'sum': 36792145.0}, 'recommendation': {'rc_unit': 49800, 'rc_upfront': 17031.6, 'od_only_rate': 5459.95, 'mixed_rate_total': 3210.49, 'mixed_rate_excl_upfront': 1763.97, 'percent_savings_over_od': 41.2}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 6488.0, 'median': 19643.0, 'max': 44344.0, 'average': 20672.1, 'std_dev': 6019.0, 'sum': 15380042.0}, 'recommendation': {'rc_unit': 20300, 'rc_upfront': 34794.2, 'od_only_rate': 11411.99, 'mixed_rate_total': 6600.07, 'mixed_rate_excl_upfront': 3644.95, 'percent_savings_over_od': 42.17}}, '_totals': {'od_only_rate': 16871.95, 'mixed_rate_total': 9810.57, 'mixed_rate_excl_upfront': 5408.92, 'rc_upfront': 51825.8, 'percent_savings_over_od': 41.85}}, 'GRU': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 4568.0, 'median': 17256.0, 'max': 54485.0, 'average': 19893.97, 'std_dev': 8236.51, 'sum': 14801117.0}, 'recommendation': {'rc_unit': 18500, 'rc_upfront': 8325.0, 'od_only_rate': 2886.22, 'mixed_rate_total': 1812.13, 'mixed_rate_excl_upfront': 1105.07, 'percent_savings_over_od': 37.21}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 3139.0, 'median': 8060.5, 'max': 17330.0, 'average': 8219.43, 'std_dev': 2993.42, 'sum': 6115255.0}, 'recommendation': {'rc_unit': 8500, 'rc_upfront': 19125.0, 'od_only_rate': 5962.37, 'mixed_rate_total': 3656.63, 'mixed_rate_excl_upfront': 2032.32, 'percent_savings_over_od': 38.67}}, '_totals': {'od_only_rate': 8848.59, 'mixed_rate_total': 5468.76, 'mixed_rate_excl_upfront': 3137.39, 'rc_upfront': 27450.0, 'percent_savings_over_od': 38.2}}, 'PDX': {'rcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 263573.0, 'median': 926059.5, 'max': 2109570.0, 'average': 994410.99, 'std_dev': 358251.01, 'sum': 739841775.0}, 'recommendation': {'rc_unit': 966200, 'rc_upfront': 289860.0, 'od_only_rate': 96179.43, 'mixed_rate_total': 57704.48, 'mixed_rate_excl_upfront': 33086.23, 'percent_savings_over_od': 40.0}}, 'wcu': {'_meta': {'rc_term': '1'}, 'stats': {'min': 111231.0, 'median': 356487.0, 'max': 739259.0, 'average': 363267.12, 'std_dev': 107277.49, 'sum': 270270736.0}, 'recommendation': {'rc_unit': 367100, 'rc_upfront': 550650.0, 'od_only_rate': 175675.98, 'mixed_rate_total': 101890.51, 'mixed_rate_excl_upfront': 55122.98, 'percent_savings_over_od': 42.0}}, '_totals': {'od_only_rate': 271855.41, 'mixed_rate_total': 159594.99, 'mixed_rate_excl_upfront': 88209.21, 'rc_upfront': 840510.0, 'percent_savings_over_od': 41.29}}, '_totals': {'od_only_rate': 865810.84, 'mixed_rate_total': 508801.27, 'rc_upfront': 2682201.1, 'percent_savings_over_od': 41.23, 'od_only_rate_yr': 10389730.02, 'mixed_rate_total_yr': 6105615.28}}
