-- Active: 1763669538843@@47.254.53.128@10000@dw

-- 步骤 1: 动态计算需要开始更新的目标日期
-- 修正: 去掉引号，避免变量替换后出现双重引号导致 ParseException
SET start_date = 20251115;

WITH
    -- CTE 1: 找出所有符合条件的点评订单
    order_sessions AS (
        SELECT
            gio.visituserid,
            gio.`time` AS order_creation_time,
            gio.time_local,
            gio.eventvariable_orderid,
            CONCAT(
                COALESCE(gio.eventvariable_country, ''),
                COALESCE(
                    gio.eventvariable_wechatid,
                    ''
                )
            ) AS W_ID,
            gio.dt
        FROM dw_gio.gio_custom_event_data_stat AS gio
        WHERE
            gio.dt >= '${hiveconf:start_date}'
            AND gio.eventvariable_orderid IS NOT NULL
            AND gio.eventvariable_orderid LIKE 'LG%' -- LG开头的是点评订单, 包括团购和到店
            AND gio.eventname = 'DPOrderCreated'
    ),
    -- CTE 2: 筛选出需要追踪的事件映射关系
    event_mapping_filtered AS (
        SELECT
            value1 AS event_name,
            value2 AS source_name
        FROM dws_analysis.William_variable
        WHERE
            value3 = 'TRUE'
            and variable_name = 'dianping_event_mapping'
    ),
    -- CTE 3: 为每笔订单找出其下单前1小时内的所有相关事件，并按时间倒序排名
    ranked_preceding_events AS (
        SELECT os.eventvariable_orderid, gio.eventname, COALESCE(
                NULLIF(
                    get_json_object(
                        gio.eventvariable, '$.listName'
                    ), ''
                ), NULLIF(
                    get_json_object(gio.eventvariable, '$.title'), ''
                )
            ) as list_name, ROW_NUMBER() OVER (
                PARTITION BY
                    os.eventvariable_orderid
                ORDER BY CAST(gio.`time` AS BIGINT) DESC
            ) as rn
        FROM
            dw_gio.gio_custom_event_data_stat AS gio
            INNER JOIN order_sessions AS os ON gio.visituserid = os.visituserid
            INNER JOIN event_mapping_filtered AS emf ON gio.eventname = emf.event_name
        WHERE
            CAST(gio.`time` AS BIGINT) < CAST(
                os.order_creation_time AS BIGINT
            )
            AND CAST(gio.`time` AS BIGINT) > (
                CAST(
                    os.order_creation_time AS BIGINT
                ) - 3600000
            )
            AND gio.dt >= '${hiveconf:start_date}'
    ),
    -- CTE 4: 将订单信息与排名第一(即最近的)的前置事件进行关联
    final_data AS (
        SELECT
            os.*,
            CASE
                WHEN rpe.list_name IS NOT NULL
                AND rpe.list_name != '' THEN CONCAT(
                    em.source_name,
                    '.',
                    rpe.list_name
                )
                ELSE COALESCE(em.source_name, 'Not Found')
            END AS Order_Source_List,
            COALESCE(em.source_name, 'Not Found') AS Order_Source
        FROM
            order_sessions AS os
            LEFT JOIN ranked_preceding_events AS rpe ON os.eventvariable_orderid = rpe.eventvariable_orderid
            AND rpe.rn = 1
            LEFT JOIN
            -- **修正**: 使用预先筛选过的 event_mapping_filtered 进行关联
            event_mapping_filtered AS em ON rpe.eventname = em.event_name
    )
    -- 主查询：从最终的CTE中选择所有数据
SELECT *
FROM final_data
LIMIT 100000;