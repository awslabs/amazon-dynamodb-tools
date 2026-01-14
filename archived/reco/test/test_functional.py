import copy
from unittest import TestCase
from functools import lru_cache
from ddb_rc_reco.reco import get_range_time, region_list, make_a_wish, generate_reco_tables, open_file_read
from ddb_rc_reco.reco import get_region_for_usage_type, process_csv, generate_hours_for_regions, parse_dt, test_file_loc


class test_full(TestCase):
    test_csv_apn1 = 'test/APN1.csv'
    def setUp(self):
        pass
    #TODO add functional test for GZIP
    def test_reco_tables(self):
        region_hours = _get_region_hours()
        wish = make_a_wish(region_hours, 1)
        reco_table = generate_reco_tables(region_hours, wish)
        stats = reco_table['NRT']['rcu']['stats']
        self.assertEqual(stats['min'], 8843.0)
        self.assertEqual(stats['median'], 48226.0)
        self.assertEqual(stats['max'], 97725.0)
        self.assertEqual(stats['average'], 49451.81)
        self.assertEqual(stats['std_dev'], 15766.74)
        self.assertEqual(stats['sum'], 36792145.0)
    def test_make_a_wish(self):
        region_hours = _get_region_hours()
        wish = make_a_wish(region_hours, 1)

        self.assertEqual(wish['NRT']['rcu']['sim_result'], 49800)
        self.assertEqual(wish['CMH']['rcu']['sim_result'], 0)
        self.assertEqual(wish['IAD']['rcu']['sim_result'], 647700)
        self.assertEqual(len(wish.keys()), 16)

@lru_cache(maxsize=10)
def _get_region_hours(csv_loc=test_file_loc):
    start_time = None
    end_time = None
    found_regions = None
    region_hours = None
    import csv
    with open_file_read(csv_loc) as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        start_time, end_time = get_range_time(row_reader)
    with open_file_read(csv_loc) as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        found_regions = region_list(row_reader)
        region_hours = generate_hours_for_regions(start_time, end_time, found_regions)
    with open_file_read(csv_loc) as csvfile:
        csv_iterator = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csv_iterator) # dump header
        process_csv(csv_iterator, region_hours)
    return region_hours        
