--v2024.9.30

WITH parameters AS (
    -- Step 1: Define the input parameters for the query
    SELECT
        3 AS months_to_scan, -- Number of months to analyze (1-6). Set to 0 to use custom date range.
                             -- Example: 3
                             -- Valid range: 0-6
                             -- Issues: Values > 6 may cause performance problems or timeout errors.
                             --         Values < 0 will cause an error.

        'YYYY-MM-DD' AS custom_start_date, -- Start date when months_to_scan is 0. Format: 'YYYY-MM-DD'
                                           -- Example: '2023-01-01'
                                           -- Valid range: Any date, but typically within the last 12-18 months
                                           -- Issues: Dates too far in the past may lack data.
                                           --         Future dates will yield no results.

        'YYYY-MM-DD' AS custom_end_date, -- End date when months_to_scan is 0. Format: 'YYYY-MM-DD'
                                         -- Example: '2023-03-31'
                                         -- Valid range: Any date after custom_start_date, up to current date
                                         -- Issues: Dates before custom_start_date will cause an error.
                                         --         Future dates will be treated as the current date.

        50 AS min_savings_per_month, -- Minimum monthly savings threshold in dollars.
                                     -- Example: 50
                                     -- Valid range: 0-1000000
                                     -- Note: 0 will show all results (will not filter any tables)
                                     -- Issues: Very high values may filter out all results.
                                     --         Negative values will cause unexpected behavior.

        ARRAY['ALL'] AS account_ids, -- AWS account IDs to analyze. Use 'ALL' for all accounts or specify IDs.
                                     -- Example: ARRAY['123456789012', '234567890123']
                                     -- Valid range: 'ALL' or any number of valid 12-digit AWS account IDs
                                     -- Issues: Invalid account IDs will be ignored.
                                     --         Empty array will yield no results.

        ARRAY['ALL'] AS payer_ids, -- Payer account IDs to analyze. Use 'ALL' for all payers or specify IDs.
                                   -- Example: ARRAY['123456789012', '234567890123']
                                   -- Valid range: 'ALL' or any number of valid 12-digit AWS account IDs
                                   -- Issues: Invalid payer IDs will be ignored.
                                   --         Empty array will yield no results.

        ARRAY['ALL'] AS table_names, -- DynamoDB table names to analyze. Use 'ALL' for all tables or specify names.
                                     -- Example: ARRAY['users', 'orders', 'products']
                                     -- Valid range: 'ALL' or any number of valid DynamoDB table names
                                     -- Issues: Non-existent table names will be ignored.
                                     --         Empty array will yield no results.

        ARRAY['ALL'] AS region_names, -- AWS regions to analyze. Use 'ALL' for all regions or specify region names.
                                      -- Example: ARRAY['us-east-1', 'eu-west-1', 'ap-southeast-2']
                                      -- Valid range: 'ALL' or any number of valid AWS region names
                                      -- Issues: Invalid region names will be ignored.
                                      --         Empty array will yield no results.

        'NET' AS cost_type, -- Cost type to use. Options: 'NET' or 'GROSS'
                            -- NET: line_item_net_unblended_cost, GROSS: line_item_unblended_cost
                            -- Valid values: 'NET', 'GROSS'
                            -- Issues: Any other value will cause an error.

        'DETAILED' AS report_type -- Report type to generate. Options: 'DETAILED' or 'SUMMARY'
                                  -- DETAILED: Individual table results, SUMMARY: Overall summary
                                  -- Valid values: 'DETAILED', 'SUMMARY'
                                  -- Issues: Any other value will cause an error.
),
date_range AS (
    -- Step 2: Calculate the start and end dates for the analysis based on the parameters
    -- This step converts the months_to_scan or custom dates into YYYYMM format for easier comparison
    SELECT
        CASE
            WHEN months_to_scan = 0 THEN DATE_FORMAT(DATE(custom_start_date), '%Y%m')
            ELSE DATE_FORMAT(DATE_ADD('month', -months_to_scan, DATE_TRUNC('month', CURRENT_DATE)), '%Y%m')
        END AS start_ym,
        CASE
            WHEN months_to_scan = 0 THEN DATE_FORMAT(DATE(custom_end_date), '%Y%m')
            ELSE DATE_FORMAT(DATE_ADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)), '%Y%m')
        END AS end_ym
    FROM parameters
),
filtered_data AS (
    -- Step 3: Filter the raw data based on the parameters and date range
    -- This step applies all the filtering conditions to reduce the dataset for further processing
    SELECT
        bill_payer_account_id AS payer_id,
        line_item_usage_account_id AS account_id,
        line_item_resource_id AS resource_arn,
        product_region AS region,
        CASE
            WHEN line_item_resource_id LIKE 'arn:aws:dynamodb:%:table/%'
            THEN REVERSE(SPLIT_PART(REVERSE(line_item_resource_id), '/', 1))
            ELSE 'UNKNOWN'
        END AS table_name,
        pricing_term,
        CASE
            WHEN parameters.cost_type = 'NET' THEN line_item_net_unblended_cost
            ELSE line_item_unblended_cost
        END AS cost,
        line_item_usage_type,
        line_item_usage_start_date,
        line_item_usage_end_date
    FROM [CUR_DB].[CUR_TABLE], date_range, parameters
    WHERE line_item_product_code = 'AmazonDynamoDB'
        AND line_item_resource_id LIKE 'arn:aws:dynamodb%'
        AND line_item_resource_id NOT LIKE '%backup%'
        AND line_item_operation in ('StandardStorage','PayPerRequestThroughput','CommittedThroughput')
        AND CAST(year AS VARCHAR) || LPAD(month, 2, '0') BETWEEN date_range.start_ym AND date_range.end_ym
        AND line_item_line_item_type NOT IN ('Tax')
        -- The CARDINALITY(FILTER()) function is used to check if 'ALL' is in the array or if a specific value matches
        -- It returns the count of elements that match the condition. If > 0, it means there's a match
        AND (CARDINALITY(FILTER(parameters.account_ids, x -> x = 'ALL')) > 0
             OR CARDINALITY(FILTER(parameters.account_ids, x -> x = line_item_usage_account_id)) > 0)
        AND (CARDINALITY(FILTER(parameters.payer_ids, x -> x = 'ALL')) > 0
             OR CARDINALITY(FILTER(parameters.payer_ids, x -> x = bill_payer_account_id)) > 0)
        AND (CARDINALITY(FILTER(parameters.table_names, x -> x = 'ALL')) > 0
             OR CARDINALITY(FILTER(parameters.table_names, x -> x =
                CASE
                    WHEN line_item_resource_id LIKE 'arn:aws:dynamodb:%:table/%'
                    THEN REVERSE(SPLIT_PART(REVERSE(line_item_resource_id), '/', 1))
                    ELSE 'UNKNOWN'
                END
             )) > 0)
        AND (CARDINALITY(FILTER(parameters.region_names, x -> x = 'ALL')) > 0
             OR CARDINALITY(FILTER(parameters.region_names, x -> x = product_region)) > 0)
        AND (
            (parameters.months_to_scan != 0) OR
            (parameters.months_to_scan = 0 AND
             line_item_usage_start_date >= DATE(parameters.custom_start_date) AND
             line_item_usage_start_date <= DATE(parameters.custom_end_date))
        )
),
aggregated_data AS (
    -- Step 4: Aggregate the filtered data by payer, account, resource, region, and table
    -- This step calculates total costs for different usage types and determines if reservations are used
    SELECT
        payer_id,
        account_id,
        resource_arn,
        region,
        table_name,
        MIN(line_item_usage_start_date) AS usage_start_date,
        MAX(line_item_usage_end_date) AS usage_end_date,
        MAX(CASE WHEN pricing_term = 'Reserved' THEN 1 ELSE 0 END) AS uses_reservations,
        SUM(CASE WHEN line_item_usage_type LIKE '%RequestUnits%' OR line_item_usage_type LIKE '%CapacityUnit-Hrs%'
                 THEN CASE WHEN line_item_usage_type NOT LIKE '%IA%' THEN cost ELSE 0 END
            ELSE 0 END) AS actual_throughput_cost,
        SUM(CASE WHEN line_item_usage_type LIKE '%TimedStorage-ByteHrs%'
                 THEN CASE WHEN line_item_usage_type NOT LIKE '%IA%' THEN cost ELSE 0 END
            ELSE 0 END) AS actual_storage_cost,
        SUM(CASE WHEN (line_item_usage_type LIKE '%RequestUnits%' OR line_item_usage_type LIKE '%CapacityUnit-Hrs%')
                      AND line_item_usage_type LIKE '%IA%'
                 THEN cost ELSE 0 END) AS actual_throughput_cost_ia,
        SUM(CASE WHEN line_item_usage_type LIKE '%TimedStorage-ByteHrs%' AND line_item_usage_type LIKE '%IA%'
                 THEN cost ELSE 0 END) AS actual_storage_cost_ia
    FROM filtered_data
    GROUP BY payer_id, account_id, resource_arn, region, table_name
),
calculated_data AS (
    -- Step 5: Perform calculations to determine potential savings and recommendations
    -- The static math here is based on AWS DynamoDB pricing and optimization strategies
    SELECT
        *,
        GREATEST(DATE_DIFF('day', usage_start_date, usage_end_date), 1) AS active_days,
        CASE
            -- For Standard tables:
            -- 0.25 and 0.6 represent the ratio of storage to throughput costs
            -- If storage cost > (0.25/0.6) * throughput cost, consider moving to Standard-IA
            WHEN uses_reservations = 0 AND (actual_storage_cost > (0.25/0.6) * NULLIF(actual_throughput_cost, 0)) THEN
                LEAST((0.6 * actual_storage_cost - 0.25 * actual_throughput_cost), 1e8)
            -- For Standard-IA tables:
            -- 0.2 and 1.5 represent the ratio of storage to throughput costs
            -- If storage cost < (0.2/1.5) * throughput cost, consider moving to Standard
            WHEN uses_reservations = 0 AND (actual_storage_cost_ia < (0.2/1.5) * NULLIF(actual_throughput_cost_ia, 0)) THEN
                LEAST((0.2 * actual_throughput_cost_ia - 1.5 * actual_storage_cost_ia), 1e8)
            ELSE 0
        END AS potential_savings,
        CASE
            -- If using reservations or potential savings are minimal, consider it optimized
            WHEN uses_reservations > 0 OR (0.6 * actual_storage_cost - 0.25 * NULLIF(actual_throughput_cost, 0)) < 0.01 AND (0.2 * NULLIF(actual_throughput_cost_ia, 0) - 1.5 * actual_storage_cost_ia) < 0.01 THEN 'Optimized'
            -- If storage cost for Standard is high compared to throughput, consider Standard-IA
            WHEN actual_storage_cost > (0.25/0.6) * NULLIF(actual_throughput_cost, 0)
                 AND (0.6 * actual_storage_cost - 0.25 * actual_throughput_cost) > 0.01 THEN 'Candidate for Standard_IA'
            -- If storage cost for Standard-IA is low compared to throughput, consider Standard
            WHEN actual_storage_cost_ia < (0.2/1.5) * NULLIF(actual_throughput_cost_ia, 0)
                 AND (0.2 * actual_throughput_cost_ia - 1.5 * actual_storage_cost_ia) > 0.01 THEN 'Candidate for Standard'
            ELSE 'Optimized'
        END AS recommendation
    FROM aggregated_data
),
result_data AS (
    -- Step 6: Format the calculated data for output
    -- This step applies the minimum savings threshold and caps values to prevent integer overflow
    SELECT
        payer_id,
        account_id,
        region,
        usage_start_date,
        usage_end_date,
        recommendation,
        table_name,
        LEAST(GREATEST(CAST(ROUND(potential_savings / (CAST(active_days AS DECIMAL(5,2)) / 30.416)) AS INTEGER), 0), 2147483647) AS potential_savings_per_month,
        LEAST(GREATEST(CAST(ROUND(potential_savings / CAST(active_days AS DECIMAL(5,1))) AS INTEGER), 0), 2147483647) AS potential_savings_per_day,
        LEAST(CAST(ROUND(COALESCE(actual_throughput_cost / (active_days / 30.416), 0)) AS INTEGER), 2147483647) AS avg_monthly_throughput_cost,
        LEAST(CAST(ROUND(COALESCE(actual_storage_cost / (active_days / 30.416), 0)) AS INTEGER), 2147483647) AS avg_monthly_storage_cost,
        LEAST(CAST(ROUND(COALESCE(actual_throughput_cost_ia / (active_days / 30.416), 0)) AS INTEGER), 2147483647) AS avg_monthly_throughput_cost_ia,
        LEAST(CAST(ROUND(COALESCE(actual_storage_cost_ia / (active_days / 30.416), 0)) AS INTEGER), 2147483647) AS avg_monthly_storage_cost_ia,
        LEAST(CAST(ROUND(COALESCE((actual_throughput_cost + actual_storage_cost + actual_throughput_cost_ia + actual_storage_cost_ia) / (active_days / 30.416), 0)) AS INTEGER), 2147483647) AS total_monthly_cost,
        resource_arn,
        CAST(uses_reservations AS INTEGER) AS uses_reservations,
        active_days,
        NULL AS total_potential_savings_ia,
        NULL AS total_potential_savings_std,
        NULL AS num_candidate_tables_to_ia,
        NULL AS num_candidate_tables_to_std
    FROM calculated_data
    WHERE potential_savings / (CAST(active_days AS DECIMAL(5,2)) / 30.416) >= (SELECT min_savings_per_month FROM parameters)
),
summary_data AS (
    -- Step 7: Calculate summary statistics from the result data
    SELECT
        CAST(SUM(CASE WHEN recommendation = 'Candidate for Standard_IA' THEN potential_savings_per_month ELSE 0 END) AS INTEGER) AS total_potential_savings_ia,
        CAST(SUM(CASE WHEN recommendation = 'Candidate for Standard' THEN potential_savings_per_month ELSE 0 END) AS INTEGER) AS total_potential_savings_std,
        CAST(COUNT(CASE WHEN recommendation = 'Candidate for Standard_IA' THEN 1 END) AS INTEGER) AS num_candidate_tables_to_ia,
        CAST(COUNT(CASE WHEN recommendation = 'Candidate for Standard' THEN 1 END) AS INTEGER) AS num_candidate_tables_to_std,
        CAST(SUM(uses_reservations) AS INTEGER) AS num_tables_ignored_for_reservations,
        CAST(SUM(avg_monthly_throughput_cost) AS INTEGER) AS total_actual_throughput_cost,
        CAST(SUM(avg_monthly_throughput_cost_ia) AS INTEGER) AS total_actual_throughput_cost_ia,
        CAST(SUM(avg_monthly_storage_cost) AS INTEGER) AS total_actual_storage_cost,
        CAST(SUM(avg_monthly_storage_cost_ia) AS INTEGER) AS total_actual_storage_cost_ia,
        MIN(usage_start_date) AS min_start_date,
        MAX(usage_end_date) AS max_end_date
    FROM result_data
)

-- Step 8: Final output - combine detailed results and summary based on the report_type parameter
SELECT * FROM (
    SELECT *
    FROM result_data
    WHERE (SELECT report_type FROM parameters) = 'DETAILED'

    UNION ALL

    SELECT
        'PAYER' AS payer_id,
        'TOTAL' AS account_id,
        'ALL' AS region,
        min_start_date AS usage_start_date,
        max_end_date AS usage_end_date,
        'Summary' AS recommendation,
        'SUMMARY' AS table_name,
        LEAST(total_potential_savings_ia + total_potential_savings_std, 2147483647) AS potential_savings_per_month,
        LEAST(CAST(ROUND((total_potential_savings_ia + total_potential_savings_std) / 30.416) AS INTEGER), 2147483647) AS potential_savings_per_day,
        total_actual_throughput_cost AS avg_monthly_throughput_cost,
        total_actual_storage_cost AS avg_monthly_storage_cost,
        total_actual_throughput_cost_ia AS avg_monthly_throughput_cost_ia,
        total_actual_storage_cost_ia AS avg_monthly_storage_cost_ia,
        LEAST(total_actual_throughput_cost + total_actual_storage_cost + total_actual_throughput_cost_ia + total_actual_storage_cost_ia, 2147483647) AS total_monthly_cost,
        'SUMMARY' AS resource_arn,
        num_tables_ignored_for_reservations AS uses_reservations,
        NULL AS active_days,
        total_potential_savings_ia,
        total_potential_savings_std,
        num_candidate_tables_to_ia,
        num_candidate_tables_to_std
    FROM summary_data
    WHERE (SELECT report_type FROM parameters) = 'SUMMARY'
)
ORDER BY
    CASE WHEN account_id = 'TOTAL' THEN 1 ELSE 0 END,
    potential_savings_per_month DESC
