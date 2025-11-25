-- 数据源:
-- dw.dw_group_order_detail, dw.dw_settlement_record, ads_analysis.user_part_data,
-- dw.dw_order_group_ticket, dw.dw_group_order_group_item, dw.dw_group_product

-- CTE 1: 订单基础数据清洗与转换 (dw_group_order_detail)

WITH
    CTE_BASE_ORDER AS (
        SELECT
            t1.order_id,
            t1.order_sn,
            t1.status,
            t1.country,
            t1.wechat_id,
            t1.city_id,
            t1.merchant_id,
            t1.pay_type,
            t1.pay_code,
            t1.item_quantity,
            t1.user_id,
            CAST(t1.created_at_local AS DATE) AS created_at_local,
            t1.created_at_local AS created_at_local_time,
            -- 金额字段除以 100 进行转换 ( dw_pay_amount, dw_total, dw_subtotal, dw_fee, dw_tax, dw_discount)
            t1.pay_amount / 100.0 AS dw_pay_amount,
            t1.total / 100.0 AS dw_total,
            t1.subtotal / 100.0 AS dw_subtotal,
            t1.fee / 100.0 AS dw_fee,
            t1.tax / 100.0 AS dw_tax,
            t1.discount / 100.0 AS dw_discount,
            -- 合并字段 M_ID 和 C_ID
            CONCAT(t1.country, t1.merchant_id) AS M_ID,
            CONCAT(
                t1.country,
                CAST(t1.city_id AS STRING)
            ) AS C_ID
        FROM dw.dw_group_order_detail t1
        WHERE
            t1.type = 'GROUP'
            AND t1.status <> 20
    ),
    -- CTE 2: 获取商品ID并判断是否为饮品 (item_id, is_drink)
    CTE_ITEM_PRODUCT AS (
        SELECT t1.order_id,
            -- item_id (DAX: RELATED(dw_group_order_group_item[item_id]))
            MAX(t2.item_id) AS item_id,
            -- is_drink (基于假设的商品类型关联)
            -- 假设 dw_group_product 中 product_name 包含 '奶茶' 或 '饮品'
            CAST(
                CASE
                    WHEN MAX(t3.product_name) LIKE '%奶茶%'
                    OR MAX(t3.product_name) LIKE '%饮品%' THEN TRUE
                    ELSE FALSE
                END AS BOOLEAN
            ) AS is_drink
        FROM
            CTE_BASE_ORDER t1
            LEFT JOIN dw.dw_group_order_group_item t2 ON t1.order_id = CAST(t2.order_id AS BIGINT)
            LEFT JOIN dw.dw_group_product t3 ON CAST(t2.item_id AS BIGINT) = t3.product_id
        GROUP BY
            t1.order_id
    ),
    -- CTE 3: 结算记录聚合与收入清洗 (dw_settlement_record)
    CTE_SETTLEMENT_AGG AS (
        SELECT
            t2.order_id,
            t2.country,
            -- 中间 Liability 金额 (用于后续计算 Amount_X，但不在最终结果中输出)
            SUM(
                CASE
                    WHEN t2.record_type = 'PAY' THEN t2.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_pay,
            SUM(
                CASE
                    WHEN t2.record_type = 'USE' THEN t2.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_use,
            SUM(
                CASE
                    WHEN t2.record_type = 'EXPIRE_NO_SETTLED' THEN t2.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_no_settled,
            SUM(
                CASE
                    WHEN t2.record_type = 'EXPIRE_SETTLED' THEN t2.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_settled,
            SUM(
                CASE
                    WHEN t2.record_type = 'REFUND' THEN t2.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_refund,
            -- pay_count (DAX: COUNTROWS filtered by record_type = 'PAY') [1]
            SUM(
                CASE
                    WHEN t2.record_type = 'PAY' THEN 1
                    ELSE 0
                END
            ) AS pay_count,
            -- 收入组件计算 (需 JSON 解析和 GST_RATE 逆算)
            -- GST_RATE 逻辑 (DAX SWITCH)
            (
                CASE t2.country
                    WHEN 'GB' THEN 1.2
                    WHEN 'AU' THEN 1.0
                    ELSE 1.0
                END
            ) AS GST_RATE,
            -- revenue_for_commission (DAX: SUM(main_settlement_record[revenue]))
            SUM(t2.revenue / 100.0) AS revenue_for_commission_calculated,
            -- revenue_commission: SUM(dw_commission / GST_RATE)
            SUM(
                COALESCE(
                    CAST(
                        GET_JSON_OBJECT(t2.metadata, '$.commission') AS DOUBLE
                    ) / 100.0 / (
                        CASE t2.country
                            WHEN 'GB' THEN 1.2
                            WHEN 'AU' THEN 1.0
                            ELSE 1.0
                        END
                    ),
                    0
                )
            ) AS revenue_commission_calculated,
            -- revenue_subsidy_ticket: SUM(-dw_subsidy / GST_RATE)
            SUM(
                COALESCE(
                    - CAST(
                        GET_JSON_OBJECT(t2.metadata, '$.subsidy') AS DOUBLE
                    ) / 100.0 / (
                        CASE t2.country
                            WHEN 'GB' THEN 1.2
                            WHEN 'AU' THEN 1.0
                            ELSE 1.0
                        END
                    ),
                    0
                )
            ) AS revenue_subsidy_ticket_calculated,
            -- revenue_subsidy_order
            SUM(
                CASE
                    WHEN t2.record_type = 'PAY' THEN t2.revenue / 100.0
                    ELSE 0
                END
            ) AS revenue_subsidy_order_calculated,
            -- revenue_base_total (DAX: SUM(revenue_ft) 的近似值，即 SUM(revenue/100.0))
            SUM(t2.revenue / 100.0) AS revenue_base_total,
            -- revenue_card_fees (假设从 metadata/其他字段获取，此处使用占位符 0)
            CAST(0 AS DOUBLE) AS revenue_card_fees_calculated
        FROM dw.dw_settlement_record t2
        WHERE
            t2.order_type = 'GROUP'
        GROUP BY
            t2.order_id,
            t2.country
    ),
    -- CTE 4: 退款记录细分 (liability_refund_expire)
    CTE_REFUND_SPLIT AS (
        SELECT t2.order_id,
            -- liability_refund_expire (DAX: [expire_date] <= [created_at])
            SUM(
                CASE
                    WHEN t1.record_type = 'REFUND'
                    AND CAST(t3.expire_date AS DATE) <= CAST(t1.created_at AS DATE) THEN t1.merchant_liability / 100.0
                    ELSE 0
                END
            ) AS liability_refund_expire
        FROM
            dw.dw_settlement_record t1
            INNER JOIN CTE_BASE_ORDER t2 ON t1.order_id = t2.order_id
            LEFT JOIN dw.dw_order_group_ticket t3 ON CAST(t1.order_id AS STRING) = t3.order_id
        WHERE
            t1.order_type = 'GROUP'
            AND t1.record_type = 'REFUND'
        GROUP BY
            t2.order_id
    ),
    -- CTE 5: 订单税率及金额计算 (联接所有中间数据)
    CTE_AMOUNT_CALCS AS (
        SELECT
            t1.*,
            t_prod.item_id,
            t_prod.is_drink,
            -- 计算 tax_rate
            CASE
                WHEN t1.dw_subtotal = 0
                OR t1.dw_subtotal IS NULL THEN 0
                ELSE t1.dw_tax / t1.dw_subtotal
            END AS tax_rate,
            -- 中间 Liability/Revenue 字段 (用于内部计算，不输出)
            t2.pay_count,
            t2.liability_pay,
            t2.liability_use,
            t2.liability_no_settled,
            t2.liability_settled,
            t2.liability_refund,
            t2.revenue_base_total,
            t2.revenue_for_commission_calculated,
            t2.revenue_commission_calculated,
            t2.revenue_subsidy_ticket_calculated,
            t2.revenue_subsidy_order_calculated,
            t2.revenue_card_fees_calculated,
            t3.liability_refund_expire
        FROM
            CTE_BASE_ORDER t1
            LEFT JOIN CTE_SETTLEMENT_AGG t2 ON t1.order_id = t2.order_id
            LEFT JOIN CTE_REFUND_SPLIT t3 ON t1.order_id = t3.order_id
            LEFT JOIN CTE_ITEM_PRODUCT t_prod ON t1.order_id = t_prod.order_id
    ),
    -- CTE 6: 最终金额和收入计算 (Amount, Final Revenue)
    CTE_FINAL_AMOUNT_CALCS AS (
        SELECT
            t1.*,
            -- Amount (DAX: [Amount]) [2]
            ROUND(
                COALESCE(
                    t1.liability_pay / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount,
            -- 仅用于计算 item_X/Amount_left，因此保留在此 CTE
            ROUND(
                COALESCE(
                    t1.liability_use / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_USE,
            ROUND(
                COALESCE(
                    t1.liability_no_settled / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_NO_SETTLED,
            ROUND(
                COALESCE(
                    t1.liability_settled / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_SETTLED,
            ROUND(
                COALESCE(
                    t1.liability_refund / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_REFUND,
            -- Amount 退款细分
            ROUND(
                COALESCE(
                    t1.liability_refund_expire / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_REFUND_expire,
            ROUND(
                COALESCE(
                    (
                        t1.liability_refund - t1.liability_refund_expire
                    ) / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount_REFUND_by_user,
            -- 最终收入指标
            t1.revenue_base_total + t1.revenue_card_fees_calculated AS revenue, -- [revenue]+[revenue_card_fees]
            t1.revenue_for_commission_calculated AS revenue_for_commission,
            t1.revenue_commission_calculated AS revenue_commission,
            t1.revenue_card_fees_calculated AS revenue_card_fees,
            t1.revenue_subsidy_ticket_calculated + t1.revenue_subsidy_order_calculated AS revenue_subsidy,
            -- revenue_others (DAX: revenue - commission - subsidy_ticket - subsidy_order)
            t1.revenue_base_total - t1.revenue_commission_calculated - t1.revenue_subsidy_ticket_calculated - t1.revenue_subsidy_order_calculated AS revenue_others
        FROM CTE_AMOUNT_CALCS t1
    ),
    -- CTE 7: 订单排名和用户数据关联 (用户属性, 排名，新老客判断)
    CTE_USER_RANKING AS (
        SELECT
            t1.*,
            -- 关联 user_part_data
            t2.first_order_order_time, -- 中间字段，需移除
            -- 计算 order_rank (DAX: RANKX with Dense) [3]
            DENSE_RANK() OVER (
                PARTITION BY
                    t1.user_id
                ORDER BY t1.created_at_local_time ASC
            ) AS order_rank,
            -- 计算 next_order_date (DAX: CALCULATE(MIN) [4])
            LEAD(t1.created_at_local, 1) OVER (
                PARTITION BY
                    t1.user_id
                ORDER BY t1.created_at_local_time ASC
            ) AS next_order_date,
            -- 计算 user_type (DAX: IF([order_rank] = 1,"New","Returning")) [5]
            CASE
                WHEN DENSE_RANK() OVER (
                    PARTITION BY
                        t1.user_id
                    ORDER BY t1.created_at_local_time ASC
                ) = 1 THEN 'New'
                ELSE 'Returning'
            END AS user_type,
            -- new_b4_delivery (DAX)
            (
                (
                    DENSE_RANK() OVER (
                        PARTITION BY
                            t1.user_id
                        ORDER BY t1.created_at_local_time ASC
                    ) = 1
                )
                AND (
                    t2.first_order_order_time IS NULL
                    OR CAST(
                        t2.first_order_order_time AS TIMESTAMP
                    ) > t1.created_at_local_time
                )
            ) AS new_b4_delivery,
            -- new_to_delivery (DAX) [6]
            (
                (
                    DENSE_RANK() OVER (
                        PARTITION BY
                            t1.user_id
                        ORDER BY t1.created_at_local_time ASC
                    ) = 1
                )
                AND (
                    CAST(
                        t2.first_order_order_time AS TIMESTAMP
                    ) > t1.created_at_local_time
                )
            ) AS new_to_delivery
        FROM
            CTE_FINAL_AMOUNT_CALCS t1
            LEFT JOIN ads_analysis.user_part_data t2 ON t1.user_id = t2.user_tid
    ),
    -- CTE 8: 计算日期差异和 Days_To_Next_Non_Drink
    CTE_RANKING_DRINK AS (
        SELECT
            t1.*,
            -- next_order_date_diff (DAX: DATEDIFF(created_at_local, next_order_date, DAY))
            DATEDIFF(
                t1.next_order_date,
                t1.created_at_local
            ) AS next_order_date_diff,
            -- Days_To_Next_Non_Drink (复杂 DAX 窗口函数，查找下一个非饮品订单)
            -- 注意：Hive 模拟 MIN OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
            DATEDIFF(
                MIN(
                    CASE
                        WHEN t1.is_drink = FALSE
                        AND t1.created_at_local > t1.created_at_local THEN t1.created_at_local
                        ELSE NULL
                    END
                ) OVER (
                    PARTITION BY
                        t1.user_id
                    ORDER BY t1.created_at_local_time ASC ROWS BETWEEN 1 FOLLOWING
                        AND UNBOUNDED FOLLOWING
                ),
                t1.created_at_local
            ) AS Days_To_Next_Non_Drink
        FROM CTE_USER_RANKING t1
    ),
    -- CTE 9: 最终计算字段 (item_left, status_CHN, has_amount_left)
    CTE_FINAL_CALCULATED_METRICS AS (
        SELECT
            t1.*,
            -- Amount_left (DAX) [7]
            ROUND(
                t1.Amount + t1.Amount_NO_SETTLED + t1.Amount_SETTLED + t1.Amount_REFUND + t1.Amount_USE,
                2
            ) AS Amount_left,
            -- has_amount_left (DAX: [Amount_left] > 0) [8]
            (
                ROUND(
                    t1.Amount + t1.Amount_NO_SETTLED + t1.Amount_SETTLED + t1.Amount_REFUND + t1.Amount_USE,
                    2
                ) > 0
            ) AS has_amount_left,
            -- item_X 数量指标 (用于计算 item_left, DAX: ROUND(DIVIDE([Amount_X],[Amount])*[item_quantity], 2)) [9, 10]
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_USE / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_Use,
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_NO_SETTLED / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_No_settled,
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_SETTLED / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_Settled,
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_REFUND / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_refund,
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_REFUND_expire / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_refund_expire,
            ROUND(
                (
                    CASE
                        WHEN t1.Amount = 0
                        OR t1.Amount IS NULL THEN 0
                        ELSE t1.Amount_REFUND_by_user / t1.Amount
                    END
                ) * t1.item_quantity,
                2
            ) AS item_refund_by_user,
            -- item_left
            t1.item_quantity - (
                ROUND(
                    (
                        CASE
                            WHEN t1.Amount = 0
                            OR t1.Amount IS NULL THEN 0
                            ELSE t1.Amount_USE / t1.Amount
                        END
                    ) * t1.item_quantity,
                    2
                ) + ROUND(
                    (
                        CASE
                            WHEN t1.Amount = 0
                            OR t1.Amount IS NULL THEN 0
                            ELSE t1.Amount_NO_SETTLED / t1.Amount
                        END
                    ) * t1.item_quantity,
                    2
                ) + ROUND(
                    (
                        CASE
                            WHEN t1.Amount = 0
                            OR t1.Amount IS NULL THEN 0
                            ELSE t1.Amount_SETTLED / t1.Amount
                        END
                    ) * t1.item_quantity,
                    2
                ) + ROUND(
                    (
                        CASE
                            WHEN t1.Amount = 0
                            OR t1.Amount IS NULL THEN 0
                            ELSE t1.Amount_REFUND / t1.Amount
                        END
                    ) * t1.item_quantity,
                    2
                )
            ) AS item_left,
            -- status_CHN (中文状态)
            CASE t1.status
                WHEN 1 THEN '待支付'
                WHEN 9 THEN '完成'
                WHEN 10 THEN '已退款'
                ELSE '其他' -- status = 20 (已取消) 已在 CTE 1 中排除
            END AS status_CHN
        FROM CTE_RANKING_DRINK t1
    ),
    -- CTE 10: 最终结果集选择 (移除所有中间计算字段)
    final AS (
        SELECT
            -- 基础信息 (C1)
            t1.order_id,
            t1.order_sn,
            t1.status,
            t1.country,
            t1.wechat_id,
            t1.city_id,
            t1.merchant_id,
            t1.pay_type,
            t1.pay_code,
            t1.item_quantity,
            t1.user_id,
            t1.created_at_local,
            t1.created_at_local_time,
            -- 基础 DW 金额 (C1)
            t1.dw_pay_amount,
            t1.dw_total,
            t1.dw_subtotal,
            t1.dw_fee,
            t1.dw_tax,
            t1.dw_discount,
            -- 衍生 ID (C1)
            t1.M_ID,
            t1.C_ID,
            -- 关键指标 & 排名 (C4, C7, C9)
            t1.tax_rate,
            t1.Amount, -- 支付金额 (扣税) [2]
            t1.Amount_USE,
            t1.Amount_NO_SETTLED,
            t1.Amount_SETTLED,
            t1.Amount_REFUND,
            t1.Amount_REFUND_expire,
            t1.Amount_REFUND_by_user,
            t1.Amount_left, -- [7]
            t1.item_Use,
            t1.item_No_settled,
            t1.item_Settled,
            t1.item_refund,
            t1.item_refund_expire,
            t1.item_refund_by_user,
            t1.item_left, -- 剩余券数量
            t1.pay_count,
            t1.order_rank,
            t1.next_order_date,
            t1.user_type,
            -- 收入列
            t1.revenue,
            t1.revenue_commission,
            t1.revenue_subsidy,
            -- ***** 新增/保留的列 *****
            t1.next_order_date_diff, -- C8
            t1.item_id, -- C2
            t1.status_CHN, -- C9
            t1.revenue_others, -- C6
            t1.revenue_card_fees, -- C6
            t1.has_amount_left, -- C9 [8]
            t1.new_b4_delivery, -- C7
            t1.new_to_delivery, -- C7 [6]
            t1.revenue_for_commission, -- C6
            t1.is_drink, -- C2
            t1.Days_To_Next_Non_Drink -- C8
        FROM CTE_FINAL_CALCULATED_METRICS t1
    )
    -- 最终结果集
SELECT *
FROM final
limit 10;