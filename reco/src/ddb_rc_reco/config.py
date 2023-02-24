test_file_loc = 'test/b8f1d493-9aa2-4e5b-a5e8-121b7cfa131e_shrunk.csv.gz'

wcu_regex = r'(?!.*IA-)(.*(^|-)WriteCapacityUnit.*)'
rcu_regex = r'(?!.*IA-)(.*(^|-)ReadCapacityUnit.*)'

version = '1.1.0'

#1yr RC rates listed only
pricing = {"NRT":{"ut_region_match_filter":r'^APN1-', "dto":0.14, "wcu":0.000742,"rcu":0.0001484,"rwcu":0.001113,"rc1":{"rcu":{"base":34.2,"hour":0.0029},"wcu":{"base":171.4,"hour":0.0147}}, "rc3":{"rcu":{"base":41.0,"hour":0.0018},"wcu":{"base":205.6,"hour":0.0093}}},
"SIN":{"ut_region_match_filter":r'^APS1-', "dto":0.12, "wcu":0.00074,"rcu":0.000148,"rwcu":0.00111,"rc1":{"rcu":{"base":34.2,"hour":0.0029},"wcu":{"base":171.0,"hour":0.0146}}, "rc3":{"rcu":{"base":41.04,"hour":0.0018},"wcu":{"base":205.2,"hour":0.0093}}},
"IAD":{"ut_region_match_filter":r'^(ReadCapacityUnit-Hrs|WriteCapacityUnit-Hrs|USE1-)', "dto":0.09, "wcu":0.00065,"rcu":0.00013,"rwcu":0.000975,"rc1":{"rcu":{"base":30,"hour":0.0025},"wcu":{"base":150,"hour":0.0128}}, "rc3":{"rcu":{"base":36,"hour":0.0016},"wcu":{"base":180,"hour":0.0081}}},
"PDX":{"ut_region_match_filter":r'^USW2-', "dto":0.09, "wcu":0.00065,"rcu":0.00013,"rwcu":0.000975,"rc1":{"rcu":{"base":30,"hour":0.0025},"wcu":{"base":150,"hour":0.0128}}, "rc3":{"rcu":{"base":36,"hour":0.0016},"wcu":{"base":180,"hour":0.0081}}},
"SFO":{"ut_region_match_filter":r'^USW1-', "dto":0.09, "wcu":0.000725,"rcu":0.000145,"rwcu":0.0010875,"rc1":{"rcu":{"base":33.6,"hour":0.0028},"wcu":{"base":168.0,"hour":0.0144}}, "rc3":{"rcu":{"base":40.32,"hour":0.0018},"wcu":{"base":201.6,"hour":0.0091}}},
"DUB":{"ut_region_match_filter":r'^EU-', "dto":0.09, "wcu":0.000735,"rcu":0.000147,"rwcu":0.0011025,"rc1":{"rcu":{"base":33.9,"hour":0.0029},"wcu":{"base":169.5,"hour":0.0145}}, "rc1":{"rcu":{"base":33.9,"hour":0.0029},"wcu":{"base":169.5,"hour":0.0145}}, "rc3":{"rcu":{"base":40.68,"hour":0.0018},"wcu":{"base":203.4,"hour":0.0092}}},
"BOM":{"ut_region_match_filter":r'^APS3-', "dto":0.1093, "wcu":0.00074,"rcu":0.000148,"rwcu":None ,"rc1":{"rcu":{"base":34.2,"hour":0.0029},"wcu":{"base":171.0,"hour":0.0146}}, "rc3":{"rcu":{"base":40.68,"hour":0.0018},"wcu":{"base":203.4,"hour":0.0092}}},
"SYD":{"ut_region_match_filter":r'^APS2-', "dto":0.114, "wcu":0.00074,"rcu":0.000148,"rwcu":0.00111,"rc1":{"rcu":{"base":34.2,"hour":0.0029},"wcu":{"base":171.0,"hour":0.0146}}, "rc3":{"rcu":{"base":41.04,"hour":0.0018},"wcu":{"base":205.2,"hour":0.0093}}},
"PDT":{"ut_region_match_filter":r'^UGW1-', "dto":0.155, "wcu":0.00078 ,"rcu":0.000156 ,"rwcu":0.00117,"rc1":{"rcu":{"base":36.0,"hour":0.0031},"wcu":{"base":180,"hour":0.0122}}, "rc3":{"rcu":{"base":43.0,"hour":0.0019},"wcu":{"base":216,"hour":0.0097}}},
"OSU":{"ut_region_match_filter":r'^UGE1-', "dto":0.155, "wcu":0.00078 ,"rcu":0.000156 ,"rwcu":0.00117,"rc1":{"rcu":{"base":36.0,"hour":0.0031},"wcu":{"base":180,"hour":0.0122}}, "rc3":{"rcu":{"base":43.0,"hour":0.0019},"wcu":{"base":216,"hour":0.0097}}},
"YUL":{"ut_region_match_filter":r'^CAN1-', "dto":0.09, "wcu":0.000715,"rcu":0.000143,"rwcu":0.000975,"rc1":{"rcu":{"base":33.0,"hour":0.0028},"wcu":{"base":165.0,"hour":0.0141}}, "rc3":{"rcu":{"base":39.6,"hour":0.0018},"wcu":{"base":198.0,"hour":0.009}}},
"FRA":{"ut_region_match_filter":r'^EUC1-', "dto": 0.09, "wcu":0.000793,"rcu":0.0001586,"rwcu":0.0011895,"rc1":{"rcu":{"base":36.6,"hour":0.0032},"wcu":{"base":183.0,"hour":0.0157}}, "rc3":{"rcu":{"base":43.8,"hour":0.0019},"wcu":{"base":219.6,"hour":0.0099 }}},
"LHR":{"ut_region_match_filter":r'^EUW2-', "dto":0.09, "wcu":0.000772,"rcu":0.0001544,"rwcu":0.001158,"rc1":{"rcu":{"base":35.6,"hour":0.0031 },"wcu":{"base":178,"hour":0.0153}}, "rc3":{"rcu":{"base":42.72,"hour":0.0019},"wcu":{"base":213.57,"hour":0.0097}}},
"CDG":{"ut_region_match_filter":r'^EUW3-', "dto":0.09, "wcu":0.000772 ,"rcu":0.0001544,"rwcu":0.001158,"rc1":{"rcu":{"base": 35.6,"hour":0.0031},"wcu":{"base": 178.0,"hour":0.0153}}},
"GRU":{"ut_region_match_filter":r'^SAE1-', "dto":0.25, "wcu":0.000975,"rcu":0.000195,"rwcu":0.001158,"rc1":{"rcu":{"base":45.0,"hour":0.0038},"wcu":{"base":225.0,"hour":0.0193}}, "rc3":{"rcu":{"base":54.0,"hour":0.0024},"wcu":{"base":270.0,"hour":0.0122}}},
"CMH":{"ut_region_match_filter":r'^USE2-', "dto":0.09, "wcu":0.00065,"rcu":0.00013,"rwcu":0.000975,"rc1":{"rcu":{"base":30.0,"hour":0.0025},"wcu":{"base":150.0,"hour":0.0128}}, "rc3":{"rcu":{"base":36.0,"hour":0.0016},"wcu":{"base":180.0,"hour":0.0081}}},
"KIX":{"ut_region_match_filter":r'^APN3-', "dto":0.114, "wcu":0.000742,"rcu":0.0001484,"rwcu":0.000975,"rc1":{"rcu":{"base":34.2,"hour":0.0029},"wcu":{"base":171.4,"hour":0.0147}}},
"ICN":{"ut_region_match_filter":r'^APN2-', "dto":0.126, "wcu":0.0007049,"rcu":0.00014098,"rwcu":0.00105735,"rc1":{"rcu":{"base":32.49,"hour":0.0028},"wcu":{"base":162.83,"hour":0.014}}, "rc3":{"rcu":{"base":38.95,"hour":0.0017},"wcu":{"base":195.32,"hour":0.0088}}},
"HKG":{"ut_region_match_filter":r'^APE1-', "dto":0.12, "wcu":0.000814,"rcu":0.0001628,"rwcu":0.00105735,"rc1":{"rcu":{"base":37.62,"hour":0.0032},"wcu":{"base":188.1,"hour":0.0161}}},
"ARN":{"ut_region_match_filter":r'^EUN1-', "dto":0.09, "wcu":0.000698,"rcu":0.00014,"rwcu":0.00105735,"rc1":{"rcu":{"base":32.2,"hour":0.0028},"wcu":{"base":161.0,"hour":0.0138}}},
"CPT":{"ut_region_match_filter":r'^AFS1-', "dto":0.154, "wcu":0.00087465, "rcu":0.00017493,"rwcu":None,"rc1":{"rcu":{"base":40.341,"hour":0.0035},"wcu":{"base":201.705,"hour":0.0173}}},
"BAH":{"ut_region_match_filter":r'^MES1-', "dto":0.117, "wcu":0.0008085, "rcu":0.0001617,"rwcu":None,"rc1":{"rcu":{"base":37.29,"hour":0.0032},"wcu":{"base":186.45,"hour":0.016}}},

}

plain_report_footer =  ("Reserved capacity to own: How much RC this payer account should own. You must compare the amount to own against your current RC reservations to determine the amount to buy. See report header."+
    "\nEffective monthly rate: The cost per month, factoring in the upfront RC purchase cost. This is the 'monthly rate after first month' + (upfront cost / months in the RC term). If you already own RC, this amount will be incorrect. This amount is only valid if the time range is one month, otherwise this field is actually the effective cost over the time period selected."+
    "\nMonthly rate after first month: The price per month if you owned all the recommended RC, not including the upfront cost. This cost includes the usage above the recommended RC plus the RC hourly rate for the recommended number of units. This amount is only valid if the time range is one month, otherwise this field is actually the sum of the cost over the time period selected." +
    "\n'...savings over the public rate card': The impact of owning this much RC! This is the money you'll save if you own the suggested amount. These prices are calculated using the public pricing and do not include any credits or negotiated discounts." +
    "\n'XXX units of 100': DynamoDB reserved capacity is bought in batches of 100 units.")

plain_report_warning_top = "Please consult your account’s active reserved capacity reservations to determine the amount of capacity to own. The amounts below do not factor in what you already own. Instead, they reflect the amount you should have in your account. Please be aware reservations are not available for rWCUs, the table class S-IA, or for the on-demand capacity mode."

descriptions = {
"NRT": {"en_us": {"region": {"code": "ap-northeast-1", "short_name": "Tokyo"}}},
"SIN": {"en_us": {"region": {"code": "ap-southeast-1", "short_name": "Singapore"}}},
"IAD": {"en_us": {"region": {"code": "us-east-1", "short_name": "N. Virginia"}}},
"PDX": {"en_us": {"region": {"code": "us-west-2", "short_name": "Oregon"}}},
"SFO": {"en_us": {"region": {"code": "us-west-1", "short_name": "N. California"}}},
"DUB": {"en_us": {"region": {"code": "eu-west-1", "short_name": "Ireland"}}},
"BOM": {"en_us": {"region": {"code": "ap-south-1", "short_name": "Mumbai"}}},
"SYD": {"en_us": {"region": {"code": "ap-southeast-2", "short_name": "Sydney"}}},
"PDT": {"en_us": {"region": {"code": "us-gov-west-1", "short_name": "GovCloud West"}}},
"OSU": {"en_us": {"region": {"code": "us-gov-east-1", "short_name": "GovCloud East"}}},
"YUL": {"en_us": {"region": {"code": "ca-central-1", "short_name": "Canada (Central)"}}},
"FRA": {"en_us": {"region": {"code": "eu-central-1", "short_name": "Frankfurt"}}},
"LHR": {"en_us": {"region": {"code": "eu-west-2", "short_name": "London"}}},
"CDG": {"en_us": {"region": {"code": "eu-west-3", "short_name": "Paris"}}},
"GRU": {"en_us": {"region": {"code": "sa-east-1", "short_name": "Sao Paolo"}}},
"CMH": {"en_us": {"region": {"code": "us-east-2", "short_name": "Ohio"}}},
"KIX": {"en_us": {"region": {"code": "ap-northeast-3", "short_name": "Osaka-Local"}}},
"ICN": {"en_us": {"region": {"code": "ap-northeast-2", "short_name": "Seoul"}}},
"HKG": {"en_us": {"region": {"code": "ap-east-1", "short_name": "Hong Kong"}}},
"ARN": {"en_us": {"region": {"code": "eu-north-1", "short_name": "Stockholm"}}},
"CPT": {"en_us": {"region": {"code": "af-south-1", "short_name": "Africa (Cape Town)"}}},
"BAH": {"en_us": {"region": {"code": "me-south-1", "short_name": "Middle East (Bahrain)"}}},
"cu": {"en_us": {"rcu": {"short_name": "RCU", "name": "Read Capacity Unit (RCU)", "reserved": "Reserved Read Capacity Unit (RCU)"},
"wcu": {"short_name":"WCU", "name": "Write Capacity Unit (WCU)", "reserved": "Reserved Write Capacity Unit (WCU)"}}},
"banner": {"en_us": {"warning_top": {"text": plain_report_warning_top}, "report_footer": {"text": plain_report_footer}}}
}

ideal_sql = '''# Replace 'MyCURReport' and run this SQL script.
SELECT product_product_name as "Service",
line_item_operation as "Operation",
line_item_usage_type as "UsageType",
'' as "IntentionallyLeftBlank",
date_format(line_item_usage_start_date, '%m/%d/%y %H:%i:%S') as "StartTime",
date_format(line_item_usage_end_date, '%m/%d/%y %H:%i:%S') as "EndTime",
sum(line_item_usage_amount) as "UsageValue",
sum(line_item_unblended_cost) as "Cost",
line_item_line_item_description
FROM "MyCURReport"
WHERE product_product_name = 'Amazon DynamoDB'
AND year = '2019' AND month = '06'
GROUP BY
product_product_name,
line_item_operation,
line_item_usage_type,
line_item_usage_start_date,
line_item_usage_end_date,
line_item_line_item_description
ORDER BY
line_item_operation,
line_item_usage_type,
line_item_usage_start_date ASC
;'''
