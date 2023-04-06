import pandas as pd
from datetime import datetime, timedelta, date


def maxA(i, j):
    if i > j:
        return i
    else:
        return j


def minA(i, j):
    if i > j:
        return j
    else:
        return i


def decrease60(L):
    return any(x > y for x, y in zip(L, L[1:]))


def decrease15(L):
    return any(x > y for x, y in zip(L, L[1:]))


def estimateUnits(read, write, readutilization, writeutilization, read_min, write_min):
    # columns [metric_name,timestamp,name,units,unitps,estunit]
    if len(read) <= len(write):
        smallest_list = read
    else:
        smallest_list = write
    finalreadcu = []
    count = 0
    last_change = "read"
    finalwritecu = []
    prevread = read[0]
    prevwrite = write[0]
    finalwritecu += [prevwrite]
    finalreadcu += [prevread]
    prevread[5] = max((prevread[4] / readutilization) * 100, read_min)
    prevwrite[5] = max((prevwrite[4] / writeutilization) * 100, write_min)
    for i in range(1, len(smallest_list)):
        currentread = read[i]
        currentwrite = write[i]

        date_time_obj = currentread[1].to_pydatetime()
        midnight = date_time_obj.replace(hour=0, minute=0, second=0)
        if date_time_obj == midnight:
            count = 0

        # compare with prev val

        if i <= 2:
            currentread[5] = prevread[5]
            currentwrite[5] = prevwrite[5]
            # prevwrite = currentwrite
           # adding 1 - 2 records to final list
            finalreadcu += [currentread]
            finalwritecu += [currentwrite]
            continue
        # creating a list with last 2 records.
        last2read = [v[4] for v in list(read[i - 2: i])]
        last2write = [v[4] for v in list(write[i - 2: i])]

        last2maxread = max(last2read)
        last2maxwrite = max(last2write)
        last2minread = min(last2read)
        last2minwrite = min(last2write)
        maxVread = maxA((last2minread / readutilization) * 100, prevread[5])

        maxVwrite = maxA((last2minwrite / writeutilization)
                         * 100, prevwrite[5])
        # scale out based on last 2 min Units.

        if currentread[0] == 'ConsumedReadCapacityUnits':
            if maxVread == (last2minread / readutilization) * 100:

                currentread[5] = (last2maxread / readutilization) * 100

            else:

                currentread[5] = maxVread

        if currentwrite[0] == 'ConsumedWriteCapacityUnits':
            if maxVwrite == (last2minwrite / writeutilization) * 100:

                currentwrite[5] = (last2maxwrite / writeutilization) * 100
            else:

                currentwrite[5] = maxVwrite

        if i <= 14:
            prevread = currentread
            # print(i, current)
            finalreadcu += [currentread]
            prevwrite = currentwrite
            # print(i, current)
            finalwritecu += [currentwrite]
            continue
        # Create list from last 15 Consumed Units
        last15read = [v[4] for v in list(read[i - 15: i])]
        last15read2 = [v[5] for v in list(read[i - 15: i])]
        # print(last15)
        last15Maxread = max(last15read)
        # Create list from last 15 Consumed Units
        last15write = [v[4] for v in list(write[i - 15: i])]
        last15write2 = [v[5] for v in list(write[i - 15: i])]
        # print(last15)
        last15Maxwrite = max(last15write)
        if count < 4:
            if not decrease15(last15read2):
                currentread[5] = max(minA(
                    (last15Maxread / readutilization) * 100, currentread[5]), read_min)
                if prevread[5] > currentread[5]:

                    count += 1

            if not decrease15(last15write2):
                currentwrite[5] = max(minA(
                    (last15Maxwrite / writeutilization) * 100, currentwrite[5]), write_min)
                if prevwrite[5] > currentwrite[5]:
                    count += 1

        else:
            if i >= 60:
                # Create list from last 60 Consumed Units
                last60read = [v[5] for v in list(read[i - 60: i])]
                last60write = [v[5] for v in list(write[i - 60: i])]
                # if Table has not scale in in past 60 minutes then scale in
                if not decrease60(last60read) and not decrease60(last60write):
                    if prevread[5] > max(minA((last15Maxread / readutilization) * 100, currentread[5]), read_min) and prevwrite[5] > max(minA((last15Maxwrite / writeutilization) * 100, currentwrite[5]), write_min):
                        if last_change == "write":
                            currentread[5] = max(
                                minA((last15Maxread / readutilization) * 100, currentread[5]), read_min)
                            last_change = "read"
                        else:
                            currentwrite[5] = max(
                                minA((last15Maxwrite / writeutilization) * 100, currentwrite[5]), write_min)
                            last_change = "write"
                    else:
                        currentwrite[5] = max(
                            minA((last15Maxwrite / writeutilization) * 100, currentwrite[5]), write_min)
                        currentread[5] = max(
                            minA((last15Maxread / readutilization) * 100, currentread[5]), read_min)
                else:
                    pass

        prevread = currentread
        prevwrite = currentwrite
        # adding current row to the result list
        finalreadcu += [currentread]
        finalwritecu += [currentwrite]
    # print(finalreadcu)
    finalist = finalwritecu + finalreadcu
    return finalist


def estimate(df, readutilization, writeutilization, read_min, write_min):

    df['unitps'] = df['unit'] / 60
    df['estunit'] = 5

    name = df['name'].unique()
    finalcu = []
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
            finalcu += estimateUnits(rcu, wcu,
                                     readutilization, writeutilization, read_min, write_min)
    if len(finalcu) > 0:
        finaldf = pd.DataFrame(finalcu)
        finaldf.columns = ['metric_name', 'timestamp',
                           'name', 'unit', 'unitps', 'estunit']
        return finaldf
    else:
        return None
