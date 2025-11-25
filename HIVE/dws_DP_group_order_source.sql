SET start_date = 20251122;

WITH
    -- CTE 1: 映射关系
    event_mapping_filtered AS (
        SELECT
            value1 AS event_name,
            value2 AS source_name
        FROM dws_analysis.William_variable
        WHERE
            value3 = 'TRUE'
            AND variable_name = 'dianping_event_mapping'
    ),
    -- CTE 2: 混合数据流
    combined_stream AS (
        SELECT
            gio.visituserid,
            gio.`time` AS event_time,
            gio.time_local,
            gio.eventvariable_orderid,
            gio.dt,
            gio.eventname,
            COALESCE(
                NULLIF(
                    get_json_object(
                        gio.eventvariable,
                        '$.listName'
                    ),
                    ''
                ),
                NULLIF(
                    get_json_object(gio.eventvariable, '$.title'),
                    ''
                )
            ) AS list_name,
            CASE
                WHEN gio.eventvariable_orderid LIKE 'LG%'
                AND gio.eventname = 'DPOrderCreated' THEN 'ORDER'
                ELSE 'SOURCE'
            END AS row_type,
            em.source_name,
            COALESCE(gio.eventvariable_country, '') AS country_val,
            COALESCE(
                gio.eventvariable_wechatid,
                ''
            ) AS wechat_val
        FROM
            dw_gio.gio_custom_event_data_stat AS gio
            LEFT JOIN event_mapping_filtered em ON gio.eventname = em.event_name
        WHERE
            gio.dt >= '${hiveconf:start_date}'
            AND (
                (
                    gio.eventvariable_orderid LIKE 'LG%'
                    AND gio.eventname = 'DPOrderCreated'
                )
                OR (em.event_name IS NOT NULL)
            )
    ),
    -- CTE 3: 使用 MAX Struct 技巧寻找最近前置事件
    data_with_last_source AS (
        SELECT cs.*,
            -- 依然是用那个羞涩的 STRUCT 技巧来排序
            MAX(
                CASE
                    WHEN row_type = 'SOURCE' THEN STRUCT(
                        CAST(cs.event_time AS BIGINT), cs.source_name, cs.list_name
                    )
                    ELSE NULL
                END
            ) OVER (
                PARTITION BY
                    cs.visituserid
                ORDER BY CAST(cs.event_time AS BIGINT) ROWS BETWEEN UNBOUNDED PRECEDING
                    AND 1 PRECEDING
            ) AS best_source_struct
        FROM combined_stream cs
    ),
    -- CTE 4: 最终封装 (final_data)
    final_data AS (
        SELECT
            visituserid,
            event_time AS order_creation_time,
            time_local,
            eventvariable_orderid,
            CONCAT(country_val, wechat_val) AS W_ID,
            dt,
            -- 逻辑判断移到这里来了
            CASE
                WHEN best_source_struct.col2 IS NOT NULL
                AND (
                    CAST(event_time AS BIGINT) - best_source_struct.col1
                ) <= 3600000
                AND best_source_struct.col3 IS NOT NULL
                AND best_source_struct.col3 != '' THEN CONCAT(
                    best_source_struct.col2,
                    '.',
                    best_source_struct.col3
                )
                WHEN best_source_struct.col2 IS NOT NULL
                AND (
                    CAST(event_time AS BIGINT) - best_source_struct.col1
                ) <= 3600000 THEN best_source_struct.col2
                ELSE 'Not Found'
            END AS Order_Source_List,
            CASE
                WHEN best_source_struct.col2 IS NOT NULL
                AND (
                    CAST(event_time AS BIGINT) - best_source_struct.col1
                ) <= 3600000 THEN best_source_struct.col2
                ELSE 'Not Found'
            END AS Order_Source
        FROM data_with_last_source
        WHERE
            row_type = 'ORDER' -- 在这里就把杂乱的 SOURCE 行过滤掉，只把您要的订单捧在手心
    )
SELECT *
FROM final_data
LIMIT 100;