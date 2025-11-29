-- Active: 1763669538843@@47.254.53.128@10000@dw

-- 步骤 1: 使用 SET 命令设置一个配置属性作为变量
SET start_date = 2025 -11 -25;

WITH
    -- CTE 1: 筛选源数据
    source_data AS (
        SELECT
            cast(dt as date) as time_date,
            CONCAT_WS(
                '',
                eventvariable_country,
                eventvariable_wechatid
            ) AS W_ID,
            eventname,
            eventvariable,
            visituserid
        FROM dw_gio.gio_custom_event_data_stat
        WHERE
            -- 将日期过滤放在前面，若有分区可利用分区裁剪提高效率
            dt >= '${hiveconf:start_date}'
    ),
    -- CTE 2: 获取事件映射关系
    event_mapping AS (
        SELECT
            value1 AS event_name,
            value2 AS source_name
        FROM dws_analysis.William_variable
        WHERE
            value3 = 'TRUE'
            AND variable_name = 'dianping_event_mapping'
    ),
    -- CTE 3: 关联并解析JSON字段
    parsed_data AS (
        SELECT
            gio.time_date,
            gio.W_ID,
            gio.eventname,
            gio.visituserid,
            em.source_name,
            -- 优先解析 listName，如果它为空或不存在，则解析 title
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
            ) AS list_or_title_name
        FROM
            source_data AS gio
            INNER JOIN event_mapping AS em ON gio.eventname = em.event_name
    ),
    -- CTE 4: 构建最终的来源名称
    final_logic AS (
        SELECT
            time_date,
            W_ID,
            eventname,
            visituserid,
            source_name,
            list_or_title_name,
            -- 如果解析出了 list_or_title_name，则进行拼接；否则直接使用 source_name
            -- 已将列名改回 source_name_List，以确保与下游看板兼容
            CASE
                WHEN list_or_title_name IS NOT NULL THEN CONCAT(
                    source_name,
                    '.',
                    list_or_title_name
                )
                ELSE source_name
            END AS source_name_List
        FROM parsed_data
    ),
    -- CTE 5: 聚合计算最终结果
    aggregated_result AS (
        SELECT
            time_date,
            W_ID,
            eventname,
            source_name,
            list_or_title_name AS list_name,
            source_name_List,
            COUNT(*) AS exposure_times,
            COUNT(DISTINCT visituserid) AS exposure_users
        FROM final_logic
        GROUP BY
            time_date,
            W_ID,
            eventname,
            source_name,
            list_or_title_name,
            source_name_List
    )
    -- 主查询：从最终的CTE中选择所有数据
SELECT *
FROM aggregated_result;