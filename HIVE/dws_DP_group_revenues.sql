WITH
    card_fee_rates AS (
        -- 步骤 1: 卡费率查找表 (使用用户提供的查询)
        SELECT
            w_id AS payment_match_key,
            CAST(cardfee AS DOUBLE) AS fee_rate
        FROM dw.analysis_cardfee
    ),
    settlement_raw AS (
        -- 步骤 2: 提取和预处理基础结算数据，计算 GST_RATE 和派生键
        SELECT
            t1.id,
            t1.order_id,
            t1.country,
            t1.wechat_id,
            t1.company_id,
            t1.status,
            t1.commission_type,
            t1.record_type,
            t1.created_at, -- 原始创建时间
            CAST(t1.created_at_local AS DATE) AS created_at_local,
            CAST(
                t1.created_at_local AS TIMESTAMP
            ) AS created_at_local_time,
            t1.pay_type,
            t1.pay_code,
            -- **保留 merchant_id (用户要求)**
            t1.merchant_id,
            -- **修正的关联键 pay_types**: 使用 CONCAT_WS('', country, pay_type)
            CONCAT_WS('', t1.country, t1.pay_type) AS pay_types,
            -- 构造 M_ID 和 C_ID
            CONCAT(t1.country, t1.merchant_id) AS M_ID,
            CONCAT(t1.country, t1.city_id) AS C_ID,
            -- **计算 GST_RATE (基于常见的逻辑)**
            CASE
                WHEN t1.country = 'GB' THEN 1.2
                WHEN t1.country = 'AU' THEN 1.0 -- 假设 AU 税率因子为 1.0 (即 1/(1+0))
                ELSE 1.0
            END AS GST_RATE,
            -- 财务金额字段进行缩放 (除以 100.0)
            t1.merchant_to_company / 100.0 AS merchant_to_company,
            t1.company_to_merchant / 100.0 AS company_to_merchant,
            t1.order_amount / 100.0 AS order_amount,
            t1.service_cash / 100.0 AS service_cash,
            t1.merchant_liability / 100.0 AS merchant_liability,
            t1.tax / 100.0 AS tax,
            t1.tax2 / 100.0 AS tax2,
            t1.revenue / 100.0 AS revenue,
            t1.unearned_revenue / 100.0 AS unearned_revenue,
            t1.product_cost / 100.0 AS product_cost,
            -- 从 metadata 中解析并缩放的字段 (使用 Hive 的 get_json_object 函数)
            CAST(
                get_json_object(t1.metadata, '$.fp') AS DOUBLE
            ) / 100.0 AS fp,
            CAST(
                get_json_object(t1.metadata, '$.up') AS DOUBLE
            ) / 100.0 AS up,
            CAST(
                get_json_object(t1.metadata, '$.subtotal') AS DOUBLE
            ) / 100.0 AS dw_subtotal,
            CAST(
                get_json_object(t1.metadata, '$.commission') AS DOUBLE
            ) / 100.0 AS dw_commission,
            CAST(
                get_json_object(t1.metadata, '$.tip') AS DOUBLE
            ) / 100.0 AS tip,
            CAST(
                get_json_object(t1.metadata, '$.discount') AS DOUBLE
            ) / 100.0 AS dw_discount,
            CAST(
                get_json_object(t1.metadata, '$.subsidy') AS DOUBLE
            ) / 100.0 AS dw_subsidy,
            CAST(
                get_json_object(t1.metadata, '$.mliability') AS DOUBLE
            ) / 100.0 AS dw_mliability
        FROM dw.dw_settlement_record t1
        WHERE
            t1.order_type = 'GROUP' -- 筛选订单类型为 GROUP 的记录 [1]
    ),
    -- 定义 intermediate CTE 来处理第一次计算
    intermediate_calculation AS (
        SELECT
            raw.*,
            -- 计算 revenue_card_fees: -(scaled order_amount) * fee_rate
            COALESCE(
                - raw.order_amount * cfr.fee_rate,
                0.0
            ) AS revenue_card_fees,
            -- 计算 revenue_subsidy_order (IF([record_type] = "PAY", [revenue], 0)) [2]
            CASE
                WHEN raw.record_type = 'PAY' THEN raw.revenue
                ELSE 0.0
            END AS revenue_subsidy_order,
            -- 计算 revenue_commission (dw_commission / GST_RATE) [3]
            raw.dw_commission / raw.GST_RATE AS revenue_commission,
            -- 计算 revenue_subsidy_ticket (-dw_subsidy / GST_RATE)
            - raw.dw_subsidy / raw.GST_RATE AS revenue_subsidy_ticket,
            -- 计算 discount (dw_discount / GST_RATE) [4]
            raw.dw_discount / raw.GST_RATE AS discount
        FROM
            settlement_raw raw
            LEFT JOIN card_fee_rates cfr ON raw.pay_types = cfr.payment_match_key
    ),
    final AS (
        -- 步骤 4: 最终计算和字段选择 (确保所有 43 个字段到位)
        SELECT
            f.order_id,
            f.country,
            f.wechat_id,
            f.company_id,
            f.merchant_to_company,
            f.company_to_merchant,
            f.order_amount,
            f.service_cash,
            f.merchant_liability,
            f.tax,
            f.tax2,
            f.revenue,
            f.unearned_revenue,
            f.product_cost,
            f.status,
            f.commission_type,
            f.created_at_local,
            f.C_ID,
            f.M_ID,
            f.record_type,
            f.id,
            f.created_at_local_time,
            f.pay_types,
            f.revenue_card_fees,
            -- 占位符字段 (通常依赖外部表或复杂逻辑)
            NULL AS days_from_pay,
            f.fp,
            f.up,
            f.dw_subtotal,
            f.dw_commission,
            f.tip,
            f.dw_discount,
            f.dw_subsidy,
            f.dw_mliability,
            f.GST_RATE,
            f.revenue_commission,
            f.revenue_subsidy_ticket,
            f.discount,
            -- 计算 revenue_other: revenue - revenue_commission - revenue_subsidy_ticket - revenue_subsidy_order
            f.revenue - f.revenue_commission - f.revenue_subsidy_ticket - f.revenue_subsidy_order AS revenue_other,
            f.revenue_subsidy_order,
            -- 计算 revenue_ft (revenue + revenue_card_fees)
            f.revenue + f.revenue_card_fees AS revenue_ft,
            -- 占位符和布尔字段
            NULL AS has_order,
            CASE
                WHEN f.record_type IN (
                    'USE',
                    'EXPIRE_NO_SETTLED',
                    'EXPIRE_SETTLED'
                ) THEN TRUE
                ELSE FALSE
            END AS is_write_off,
            0 AS orders_write_off, -- 复杂计算，用 0 占位 [5]
            f.created_at,
            NULL AS refund_type, -- 复杂计算，用 NULL 占位 [6]
            -- 确保包含保留的原始 merchant_id
            f.merchant_id
        FROM intermediate_calculation f
    )
    -- 步骤 5: 严格按照要求以 SELECT * FROM final 结束
SELECT *
FROM final
where
    created_at_local = '2025-11-20'