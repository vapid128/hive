-- 步骤 1: 设置目标日期变量
SET var.target_date = '2025-11-27';

-- 步骤 2: 使用 CTE 构建分层逻辑，创建或替换最终的聚合

WITH
    -- CTE 1: 计算总曝光指标 (不受 eventname 筛选影响)
    total_exposures AS (
        SELECT
            cast(dt as date) as time_date,
            CONCAT_WS(
                '',
                eventvariable_country,
                eventvariable_wechatid
            ) AS W_ID,
            eventvariable_language AS language,
            COUNT(*) AS exposure_times,
            COUNT(DISTINCT visituserid) AS exposure_users
        FROM dw_gio.gio_custom_event_data_stat
        WHERE
            dt >= '${hiveconf:start_date}'
            AND domain = 'com.ca.fantuan.customer'
        GROUP BY
            cast(dt as date),
            CONCAT_WS(
                '',
                eventvariable_country,
                eventvariable_wechatid
            ),
            eventvariable_language
    ),
    -- CTE 2: 漏斗事件筛选与等级标记 (仅用于计算漏斗)
    filtered_events AS (
        SELECT
            cast(dt as date) as time_date,
            CONCAT_WS(
                '',
                eventvariable_country,
                eventvariable_wechatid
            ) AS W_ID,
            eventvariable_language AS language,
            visituserid,
            eventname,
            CASE
                WHEN eventname IN (
                    'DReviewHomePageView',
                    'DPHomepageView'
                ) THEN 1
                WHEN eventname = 'DReviewMerchantItemView' THEN 2
                WHEN eventname = 'DPGoodspPageView' THEN 3
                WHEN eventname = 'DPorderConfirmPageView' THEN 4
                WHEN eventname = 'DPOrderCreated' THEN 5
            END AS event_level
        FROM dw_gio.gio_custom_event_data_stat
        WHERE
            dt >= '${hiveconf:start_date}'
            AND domain = 'com.ca.fantuan.customer'
            AND eventname IN (
                'DReviewHomePageView',
                'DPHomepageView',
                'DReviewMerchantItemView',
                'DPGoodspPageView',
                'DPorderConfirmPageView',
                'DPOrderCreated'
            )
    ),
    -- CTE 3: 计算每个用户的每日最高触达等级 (用于漏斗UV)
    user_daily_max_level AS (
        SELECT
            time_date,
            W_ID,
            language,
            visituserid,
            MAX(event_level) AS max_level
        FROM filtered_events
        GROUP BY
            time_date,
            W_ID,
            language,
            visituserid
    ),
    -- CTE 4: 计算各等级的PV (仅漏斗)
    daily_pvs AS (
        SELECT
            time_date,
            W_ID,
            language,
            COUNT(
                CASE
                    WHEN event_level = 1 THEN 1
                END
            ) AS DPHomepage_PV,
            COUNT(
                CASE
                    WHEN event_level = 2 THEN 1
                END
            ) AS DPMerchan_PV,
            COUNT(
                CASE
                    WHEN event_level = 3 THEN 1
                END
            ) AS DPGoods_PV,
            COUNT(
                CASE
                    WHEN event_level = 4 THEN 1
                END
            ) AS DPConfirmPage_PV,
            COUNT(
                CASE
                    WHEN event_level = 5 THEN 1
                END
            ) AS DPOrderCreated_PV
        FROM filtered_events
        GROUP BY
            time_date,
            W_ID,
            language
    ),
    -- CTE 5: 计算各等级的UV (仅漏斗)
    daily_uvs AS (
        SELECT
            time_date,
            W_ID,
            language,
            COUNT(
                DISTINCT CASE
                    WHEN max_level >= 1 THEN visituserid
                END
            ) AS DPHomepage_UV,
            COUNT(
                DISTINCT CASE
                    WHEN max_level >= 2 THEN visituserid
                END
            ) AS DPMerchan_UV,
            COUNT(
                DISTINCT CASE
                    WHEN max_level >= 3 THEN visituserid
                END
            ) AS DPGoods_UV,
            COUNT(
                DISTINCT CASE
                    WHEN max_level >= 4 THEN visituserid
                END
            ) AS DPConfirmPage_UV,
            COUNT(
                DISTINCT CASE
                    WHEN max_level >= 5 THEN visituserid
                END
            ) AS DPOrderCreated_UV
        FROM user_daily_max_level
        GROUP BY
            time_date,
            W_ID,
            language
    ),
    -- CTE 6: 将漏斗的PV和UV指标关联起来
    funnel_metrics AS (
        SELECT pvs.time_date, pvs.W_ID, pvs.language, pvs.DPHomepage_PV, pvs.DPMerchan_PV, pvs.DPGoods_PV, pvs.DPConfirmPage_PV, pvs.DPOrderCreated_PV, uvs.DPHomepage_UV, uvs.DPMerchan_UV, uvs.DPGoods_UV, uvs.DPConfirmPage_UV, uvs.DPOrderCreated_UV
        FROM
            daily_pvs AS pvs
            INNER JOIN daily_uvs AS uvs ON pvs.time_date = uvs.time_date
            AND pvs.W_ID = uvs.W_ID
            AND pvs.language = uvs.language
    ),
    -- CTE 7: 将总曝光指标与漏斗指标进行安全合并
    unfiltered_final_data AS (
        SELECT
            total.time_date,
            total.W_ID,
            total.language,
            total.exposure_times,
            total.exposure_users,
            COALESCE(funnel.DPHomepage_PV, 0) AS DPHomepage_PV,
            COALESCE(funnel.DPMerchan_PV, 0) AS DPMerchan_PV,
            COALESCE(funnel.DPGoods_PV, 0) AS DPGoods_PV,
            COALESCE(funnel.DPConfirmPage_PV, 0) AS DPConfirmPage_PV,
            COALESCE(funnel.DPOrderCreated_PV, 0) AS DPOrderCreated_PV,
            COALESCE(funnel.DPHomepage_UV, 0) AS DPHomepage_UV,
            COALESCE(funnel.DPMerchan_UV, 0) AS DPMerchan_UV,
            COALESCE(funnel.DPGoods_UV, 0) AS DPGoods_UV,
            COALESCE(funnel.DPConfirmPage_UV, 0) AS DPConfirmPage_UV,
            COALESCE(funnel.DPOrderCreated_UV, 0) AS DPOrderCreated_UV
        FROM
            total_exposures AS total
            LEFT JOIN funnel_metrics AS funnel ON total.time_date = funnel.time_date
            AND total.W_ID = funnel.W_ID
            AND total.language = funnel.language
    ),
    -- ✨新增的最终筛选CTE✨: 只保留 DPHomepage_UV > 0 的行
    final_filtered_data AS (
        SELECT *
        FROM unfiltered_final_data
        WHERE
            DPHomepage_UV > 0
    )
    -- 从最终筛选后的CTE中查询所有数据，完成！
SELECT *
FROM final_filtered_data;