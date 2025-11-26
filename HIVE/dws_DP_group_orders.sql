-- 数据源:
-- 1. dw.dw_group_order_detail
-- 2. dw.dw_settlement_record (包含 JSON metadata)
-- 3. ads_analysis.user_part_data
-- 4. dw.dw_order_group_ticket
-- 5. dw.dw_group_order_group_item
-- 6. dw.dw_dianping_merchant_bing_category
-- 7. dw.dw_group_merchant_extra

-- CTE 1: 订单基础数据清洗与转换

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
            -- 金额字段除以 100 进行转换
            t1.pay_amount / 100.0 AS dw_pay_amount,
            t1.total / 100.0 AS dw_total,
            t1.subtotal / 100.0 AS dw_subtotal,
            t1.fee / 100.0 AS dw_fee,
            t1.tax / 100.0 AS dw_tax,
            t1.discount / 100.0 AS dw_discount,
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
    -- CTE 2: 获取商品ID并根据商户属性判断是否为饮品 (item_id, is_drink)
    CTE_ITEM_CATEGORY AS (
        SELECT t1.order_id, MAX(t2.item_id) AS item_id,
            -- is_drink 逻辑: 只要商户的 bing_category 符合 '1-9-1%' (饮品店)，则该订单标记为 TRUE
            MAX(
                CASE
                    WHEN t4.bing_category LIKE '1-9-1%' THEN TRUE
                    ELSE FALSE
                END
            ) AS is_drink
        FROM
            CTE_BASE_ORDER t1
            LEFT JOIN dw.dw_group_order_group_item t2 ON t1.order_id = CAST(t2.order_id AS BIGINT)
            LEFT JOIN dw.dw_group_merchant_extra t3 ON t1.merchant_id = CAST(t3.merchant_id AS BIGINT)
            LEFT JOIN dw.dw_dianping_merchant_bing_category t4 ON t3.review_merchant_id = t4.merchant_id
        GROUP BY
            t1.order_id
    ),
    -- CTE 3: 结算记录聚合与收入清洗
    CTE_SETTLEMENT_AGG AS (
        SELECT
            t2.order_id,
            t2.country,
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
            SUM(
                CASE
                    WHEN t2.record_type = 'PAY' THEN 1
                    ELSE 0
                END
            ) AS pay_count,
            (
                CASE t2.country
                    WHEN 'GB' THEN 1.2
                    WHEN 'AU' THEN 1.0
                    ELSE 1.0
                END
            ) AS GST_RATE,
            SUM(t2.revenue / 100.0) AS revenue_for_commission_calculated,
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
            SUM(
                CASE
                    WHEN t2.record_type = 'PAY' THEN t2.revenue / 100.0
                    ELSE 0
                END
            ) AS revenue_subsidy_order_calculated,
            SUM(t2.revenue / 100.0) AS revenue_base_total,
            CAST(0 AS DOUBLE) AS revenue_card_fees_calculated
        FROM dw.dw_settlement_record t2
        WHERE
            t2.order_type = 'GROUP'
        GROUP BY
            t2.order_id,
            t2.country
    ),
    -- CTE 4: 退款记录细分
    CTE_REFUND_SPLIT AS (
        SELECT t2.order_id, SUM(
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
    -- CTE 5: 订单税率及金额计算
    CTE_AMOUNT_CALCS AS (
        SELECT
            t1.*,
            t_cat.item_id,
            t_cat.is_drink,
            CASE
                WHEN t1.dw_subtotal = 0
                OR t1.dw_subtotal IS NULL THEN 0
                ELSE t1.dw_tax / t1.dw_subtotal
            END AS tax_rate,
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
            LEFT JOIN CTE_ITEM_CATEGORY t_cat ON t1.order_id = t_cat.order_id
    ),
    -- CTE 6: 最终金额和收入计算
    CTE_FINAL_AMOUNT_CALCS AS (
        SELECT
            t1.*,
            ROUND(
                COALESCE(
                    t1.liability_pay / (1.0 + t1.tax_rate),
                    0
                ),
                2
            ) AS Amount,
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
            t1.revenue_base_total + t1.revenue_card_fees_calculated AS revenue,
            t1.revenue_for_commission_calculated AS revenue_for_commission,
            t1.revenue_commission_calculated AS revenue_commission,
            t1.revenue_card_fees_calculated AS revenue_card_fees,
            t1.revenue_subsidy_ticket_calculated + t1.revenue_subsidy_order_calculated AS revenue_subsidy,
            t1.revenue_base_total - t1.revenue_commission_calculated - t1.revenue_subsidy_ticket_calculated - t1.revenue_subsidy_order_calculated AS revenue_others
        FROM CTE_AMOUNT_CALCS t1
    ),
    -- CTE 7: 订单排名和用户数据关联
    CTE_USER_RANKING AS (
        SELECT
            t1.*,
            DENSE_RANK() OVER (
                PARTITION BY
                    t1.user_id
                ORDER BY t1.created_at_local_time ASC
            ) AS order_rank,
            LEAD(t1.created_at_local, 1) OVER (
                PARTITION BY
                    t1.user_id
                ORDER BY t1.created_at_local_time ASC
            ) AS next_order_date,
            t2.first_order_order_time,
            CASE
                WHEN DENSE_RANK() OVER (
                    PARTITION BY
                        t1.user_id
                    ORDER BY t1.created_at_local_time ASC
                ) = 1 THEN 'New'
                ELSE 'Returning'
            END AS user_type,
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
    -- CTE 8: [修复] 计算日期差异和 Days_To_Next_Non_Drink
    CTE_RANKING_DRINK AS (
        SELECT
            t1.*,
            -- next_order_date_diff 
            DATEDIFF(
                t1.next_order_date,
                t1.created_at_local
            ) AS next_order_date_diff,
            -- Days_To_Next_Non_Drink
            -- FIX: 删除了 t1.created_at_local_time > t1.created_at_local_time 的错误判断
            DATEDIFF(
                MIN(
                    CASE
                        WHEN t1.is_drink = FALSE THEN t1.created_at_local
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
    -- CTE 9: 最终计算字段
    CTE_FINAL_CALCULATED_METRICS AS (
        SELECT
            t1.*,
            ROUND(
                t1.Amount + t1.Amount_NO_SETTLED + t1.Amount_SETTLED + t1.Amount_REFUND + t1.Amount_USE,
                2
            ) AS Amount_left,
            (
                ROUND(
                    t1.Amount + t1.Amount_NO_SETTLED + t1.Amount_SETTLED + t1.Amount_REFUND + t1.Amount_USE,
                    2
                ) > 0
            ) AS has_amount_left,
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
            CASE t1.status
                WHEN 1 THEN '待支付'
                WHEN 9 THEN '完成'
                WHEN 10 THEN '已退款'
                ELSE '其他'
            END AS status_CHN
        FROM CTE_RANKING_DRINK t1
    ),
    -- CTE 10: 最终结果集选择
    final AS (
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
            t1.created_at_local,
            t1.created_at_local_time,
            t1.dw_pay_amount,
            t1.dw_total,
            t1.dw_subtotal,
            t1.dw_fee,
            t1.dw_tax,
            t1.dw_discount,
            t1.M_ID,
            t1.C_ID,
            t1.tax_rate,
            t1.Amount,
            t1.Amount_USE,
            t1.Amount_NO_SETTLED,
            t1.Amount_SETTLED,
            t1.Amount_REFUND,
            t1.Amount_REFUND_expire,
            t1.Amount_REFUND_by_user,
            t1.Amount_left,
            t1.item_Use,
            t1.item_No_settled,
            t1.item_Settled,
            t1.item_refund,
            t1.item_refund_expire,
            t1.item_refund_by_user,
            t1.item_left,
            t1.pay_count,
            t1.order_rank,
            t1.next_order_date,
            t1.user_type,
            t1.revenue,
            t1.revenue_commission,
            t1.revenue_subsidy,
            t1.next_order_date_diff,
            t1.item_id,
            t1.status_CHN,
            t1.revenue_others,
            t1.revenue_card_fees,
            t1.has_amount_left,
            t1.new_b4_delivery,
            t1.new_to_delivery,
            t1.revenue_for_commission,
            t1.is_drink,
            t1.Days_To_Next_Non_Drink
        FROM CTE_FINAL_CALCULATED_METRICS t1
    )
SELECT *
FROM final
where
    created_at_local = '2025-11-19';