import pandas as pd
from datetime import datetime, timedelta, date


def max_a(i, j):
    return i if i > j else j


def min_a(i, j):
    return j if i > j else i


def decrease(L):
    return any(x > y for x, y in zip(L, L[1:]))


def estimate_units(read, write, read_utilization, write_utilization, read_min, write_min, read_max, write_max):
    # columns [metric_name,timestamp,name,units,unitps,estunit]
    if len(read) <= len(write):
        smallest_list = read
    else:
        smallest_list = write
    final_read_cu = []

    # Scale-in threshold = 20% percent to prevent small fluctuations in capacity usage from triggering unnecessary scale-ins.
    scale_in_threshold = 1.20
    count = 0
    last_change = "read"
    final_write_cu = []
    prev_read = read[0]
    prev_write = write[0]
    final_write_cu += [prev_write]
    final_read_cu += [prev_read]
    prev_read[5] = min(max((prev_read[4] / read_utilization)
                      * 100, read_min), read_max)
    prev_write[5] = min(max((prev_write[4] / write_utilization)
                       * 100, write_min), write_max)
    for i in range(1, len(smallest_list)):
        current_read = read[i]
        current_write = write[i]

        date_time_obj = current_read[1].to_pydatetime()
        midnight = date_time_obj.replace(hour=0, minute=0, second=0)
        if date_time_obj == midnight:
            count = 0

        # compare with prev val

        if i <= 2:
            current_read[5] = prev_read[5]
            current_write[5] = prev_write[5]
            final_read_cu += [current_read]
            final_write_cu += [current_write]
            continue
        # creating a list with last 2 records.
        last2_read = [v[4] for v in list(read[i - 2: i])]
        last2_write = [v[4] for v in list(write[i - 2: i])]

        last2_max_read = max(last2_read)
        last2_max_write = max(last2_write)
        last2_min_read = min(last2_read)
        last2_min_write = min(last2_write)
        max_vread = min(max_a((last2_min_read / read_utilization)
                            * 100, prev_read[5]), read_max)

        max_vwrite = min(max_a((last2_min_write / write_utilization)
                             * 100, prev_write[5]), write_max)
        # scale out based on last 2 min Units.

        if current_read[0] == 'ConsumedReadCapacityUnits':
            if max_vread == (last2_min_read / read_utilization) * 100:

                current_read[5] = (last2_max_read / read_utilization) * 100

            else:

                current_read[5] = max_vread

        if current_write[0] == 'ConsumedWriteCapacityUnits':
            if max_vwrite == (last2_min_write / write_utilization) * 100:

                current_write[5] = (last2_max_write / write_utilization) * 100
            else:

                current_write[5] = max_vwrite

        if i <= 14:
            prev_read = current_read
            final_read_cu += [current_read]
            prev_write = current_write
            final_write_cu += [current_write]
            continue
        # Create list from last 15 Consumed Read Units
        last15_read = [v[4] for v in list(read[i - 15: i])]
        last15_read2 = [v[5] for v in list(read[i - 15: i])]
        last15_max_read = max(last15_read)
        # Create list from last 15 Consumed Write Units
        last15_write = [v[4] for v in list(write[i - 15: i])]
        last15_write2 = [v[5] for v in list(write[i - 15: i])]
        last15_max_write = max(last15_write)
        # Scale-in based on last 15 Consumed Units
        # First 4 scale-in operation can happen anytime during the a day, there after every once an hour
        if count < 4:
            if not decrease(last15_read2):
                if prev_read[5] > (max(min_a(
                        (last15_max_read / read_utilization) * 100, current_read[5]), read_min) * scale_in_threshold):
                    current_read[5] = max(min_a(
                        (last15_max_read / read_utilization) * 100, current_read[5]), read_min)
                if prev_read[5] > current_read[5]:

                    count += 1

            if not decrease(last15_write2):
                if prev_write[5] > (max(min_a(
                        (last15_max_write / write_utilization) * 100, current_write[5]), write_min) * scale_in_threshold):
                    current_write[5] = max(min_a(
                        (last15_max_write / write_utilization) * 100, current_write[5]), write_min)
                if prev_write[5] > current_write[5]:
                    count += 1

        else:
            if i >= 60:
                # Create list from last 60 Consumed Units
                last60_read = [v[5] for v in list(read[i - 60: i])]
                last60_write = [v[5] for v in list(write[i - 60: i])]
                # if Table has not scale in in past 60 minutes then scale in
                if not decrease(last60_read) and not decrease(last60_write):
                    if prev_read[5] > (max(
                            min_a((last15_max_read / read_utilization) * 100, current_read[5]), read_min) * scale_in_threshold) and prev_write[5] > (max(min_a((last15_max_write / write_utilization) * 100, current_write[5]), write_min) * scale_in_threshold):
                        if last_change == "write":
                            current_read[5] = max(
                                min_a((last15_max_read / read_utilization) * 100, current_read[5]), read_min)
                            last_change = "read"
                        else:
                            current_write[5] = max(
                                min_a((last15_max_write / write_utilization) * 100, current_write[5]), write_min)
                            last_change = "write"
                    else:
                        if prev_read[5] > (max(
                                min_a((last15_max_read / read_utilization) * 100, current_read[5]), read_min) * scale_in_threshold):
                            current_read[5] = max(
                                min_a((last15_max_read / read_utilization) * 100, current_read[5]), read_min)

                        if prev_write[5] > (max
                                           (min_a((last15_max_write / write_utilization) * 100, current_write[5]), write_min) * scale_in_threshold):
                            current_write[5] = max(
                                min_a((last15_max_write / write_utilization) * 100, current_write[5]), write_min)

                else:
                    pass

        prev_read = current_read
        prev_write = current_write
        final_read_cu += [current_read]
        final_write_cu += [current_write]
    final_list = final_write_cu + final_read_cu
    return final_list


def estimate(df, read_utilization, write_utilization, read_min, write_min, read_max, write_max):

    df['unitps'] = df['unit'] / 60
    df['estunit'] = 5

    name = df['name'].unique()
    final_cu = []
    for table_name in name:

        rcu = df.query(
            "metric_name == 'ConsumedReadCapacityUnits' and name == @table_name")
        wcu = df.query(
            "metric_name == 'ConsumedWriteCapacityUnits' and name == @table_name")
        rcu = ((rcu.sort_values(by='timestamp', ascending=True)
                ).reset_index(drop=True)).values.tolist()
        wcu = ((wcu.sort_values(by='timestamp', ascending=True)
                ).reset_index(drop=True)).values.tolist()
        if len(rcu) > 0 and len(wcu) > 0:
            final_cu += estimate_units(rcu, wcu,
                                     read_utilization, write_utilization, read_min, write_min, read_max, write_max)
    if len(final_cu) > 0:
        final_df = pd.DataFrame(final_cu)
        final_df.columns = ['metric_name', 'timestamp',
                           'name', 'unit', 'unitps', 'estunit']
        return final_df
    else:
        return None
