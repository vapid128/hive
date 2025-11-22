-- Active: 1763669538843@@47.254.53.128@10000@dw

SET start_month = 202511;

WITH
    special_scenes AS (
        SELECT value1
        FROM dws_analysis.William_variable
        where
            variable_name = 'business_scene_order'
            and value3 = 'TRUE'
    ),
    source_with_validated_area AS (
        SELECT
            s.*,
            d.m_area_id AS deliverer_m_area_id,
            (
                d.m_area_id IS NOT NULL
                AND a.id IS NOT NULL
            ) AS is_area_validated
        FROM
            dw.dw_revenues AS s
            LEFT JOIN dw.dw_deliverers AS d ON cast(s.deliverer_id as string) = cast(d.id as string)
            AND s.country = d.country
            LEFT JOIN dw.dw_areas AS a ON d.id = a.id
            AND s.country = a.country
            AND s.wechat_id = a.wechat_id
        where
            s.month >= CAST(
                '${hiveconf:start_month}' AS INT
            )
            and s.business_scene not IN (
                SELECT value1
                FROM special_scenes
            )
    ),
    deduped_normal_data AS (
        SELECT *
        FROM (
                SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY
                            id, country
                        ORDER BY created_at DESC
                    ) as rn
                FROM source_with_validated_area
            ) t
        WHERE
            rn = 1
    ),
    final_revenues AS (
        SELECT
            CAST(s.id AS STRING) AS id,
            CAST(s.wechat_id AS BIGINT) AS wechat_id,
            CAST(s.order_id AS BIGINT) AS order_id,
            CAST(s.restaurant_id AS BIGINT) AS restaurant_id,
            CAST(s.deliverer_id AS STRING) AS deliverer_id,
            CAST(s.member_id AS BIGINT) AS member_id,
            CAST(s.pay_type AS BIGINT) AS pay_type,
            CAST(s.transfer_type AS BIGINT) AS transfer_type,
            CAST(s.dtr AS DOUBLE) AS dtr,
            CAST(s.dtc AS DOUBLE) AS dtc,
            CAST(s.ctd AS DOUBLE) AS ctd,
            CAST(s.rtc AS DOUBLE) AS rtc,
            CAST(s.ctr AS DOUBLE) AS ctr,
            CAST(s.income AS DOUBLE) AS income,
            CAST(s.revenue AS DOUBLE) AS revenue,
            CAST(s.tax AS DOUBLE) AS tax,
            CAST(s.itc AS DOUBLE) AS itc,
            CAST(s.fp AS DOUBLE) AS fp,
            CAST(s.ds AS DOUBLE) AS ds,
            CAST(s.dt AS DOUBLE) AS dt,
            CAST(s.rp AS DOUBLE) AS rp,
            CAST(s.up AS DOUBLE) AS up,
            CAST(s.rt AS DOUBLE) AS rt,
            CAST(s.rtgst AS DOUBLE) AS rtgst,
            CAST(s.rtpst AS DOUBLE) AS rtpst,
            CAST(s.ut AS DOUBLE) AS ut,
            CAST(s.utgst AS DOUBLE) AS utgst,
            CAST(s.utpst AS DOUBLE) AS utpst,
            CAST(s.zt AS DOUBLE) AS zt,
            CAST(s.yt AS DOUBLE) AS yt,
            CAST(s.ot AS DOUBLE) AS ot,
            CAST(s.shipping_distance AS DOUBLE) AS shipping_distance,
            s.created_at,
            CAST(
                s.created_at_local AS TIMESTAMP
            ) AS created_at_local,
            s.updated_at,
            CAST(
                s.updated_at_local AS TIMESTAMP
            ) AS updated_at_local,
            CAST(s.profit_type AS BIGINT) AS profit_type,
            CAST(s.restaurant_status AS BIGINT) AS restaurant_status,
            CAST(s.`desc` AS STRING) AS `desc`,
            CAST(s.desc_order AS STRING) AS desc_order,
            CAST(s.desc_info AS STRING) AS desc_info,
            CAST(s.deliverer_status AS BIGINT) AS deliverer_status,
            CAST(s.np AS DOUBLE) AS np,
            CAST(s.alipay AS DOUBLE) AS alipay,
            CAST(s.moneris AS DOUBLE) AS moneris,
            CAST(s.wechatpay AS DOUBLE) AS wechatpay,
            CAST(s.dc AS DOUBLE) AS dc,
            CAST(s.rc AS DOUBLE) AS rc,
            CAST(s.sc AS DOUBLE) AS sc,
            CAST(s.rl AS DOUBLE) AS rl,
            CAST(s.gst AS DOUBLE) AS gst,
            CAST(s.fc AS DOUBLE) AS fc,
            CAST(s.td AS DOUBLE) AS td,
            CAST(s.cd AS DOUBLE) AS cd,
            CAST(s.cr AS DOUBLE) AS cr,
            CAST(s.tr AS DOUBLE) AS tr,
            CAST(s.ra AS DOUBLE) AS ra,
            CAST(s.pr AS DOUBLE) AS pr,
            CAST(s.pd AS DOUBLE) AS pd,
            CAST(s.sca AS DOUBLE) AS sca,
            CAST(s.st AS DOUBLE) AS st,
            CAST(s.rcp AS DOUBLE) AS rcp,
            CAST(s.rct AS DOUBLE) AS rct,
            CAST(s.r_fc AS DOUBLE) AS r_fc,
            CAST(s.r_gs AS DOUBLE) AS r_gs,
            CAST(s.r_gst AS DOUBLE) AS r_gst,
            CAST(s.r_pst AS DOUBLE) AS r_pst,
            CAST(s.r_cr AS DOUBLE) AS r_cr,
            CAST(s.r_tr AS DOUBLE) AS r_tr,
            CAST(s.r_ra AS DOUBLE) AS r_ra,
            CAST(s.r_pbt AS DOUBLE) AS r_pbt,
            CAST(s.r_fee AS DOUBLE) AS r_fee,
            CAST(s.r_fgst AS DOUBLE) AS r_fgst,
            CAST(s.company_id AS BIGINT) AS company_id,
            CAST(s.restaurant_type AS STRING) AS restaurant_type,
            CAST(s.urevenue AS DOUBLE) AS urevenue,
            CAST(s.urevenue_gst AS DOUBLE) AS urevenue_gst,
            CAST(s.stripe AS DOUBLE) AS stripe,
            CAST(s.applepay AS DOUBLE) AS applepay,
            CAST(s.googlepay AS DOUBLE) AS googlepay,
            CAST(s.summary_id AS BIGINT) AS summary_id,
            CAST(
                s.braintree_credit_card AS DOUBLE
            ) AS braintree_credit_card,
            CAST(
                s.braintree_apple_pay AS DOUBLE
            ) AS braintree_apple_pay,
            CAST(s.note_type AS BIGINT) AS note_type,
            CAST(s.other_payable_r AS DOUBLE) AS other_payable_r,
            CAST(s.other_income_d AS DOUBLE) AS other_income_d,
            CAST(s.origin_alipay AS DOUBLE) AS origin_alipay,
            CAST(s.iot_alipay AS DOUBLE) AS iot_alipay,
            CAST(s.citcon_alipay AS DOUBLE) AS citcon_alipay,
            CAST(s.iot_wechatpay AS DOUBLE) AS iot_wechatpay,
            CAST(s.citcon_wechatpay AS DOUBLE) AS citcon_wechatpay,
            CAST(
                s.dynamic_shipping_cost AS DOUBLE
            ) AS dynamic_shipping_cost,
            CAST(s.bf AS DOUBLE) AS bf,
            CAST(s.bftgst AS DOUBLE) AS bftgst,
            CAST(s.bftpst AS DOUBLE) AS bftpst,
            CAST(
                s.deliverer_note_type AS STRING
            ) AS deliverer_note_type,
            CAST(s.noah_alipay AS DOUBLE) AS noah_alipay,
            CAST(s.noah_wechatpay AS DOUBLE) AS noah_wechatpay,
            CAST(s.braintree_venmo AS DOUBLE) AS braintree_venmo,
            CAST(s.braintree_paypal AS DOUBLE) AS braintree_paypal,
            CAST(s.rds AS DOUBLE) AS rds,
            CAST(s.pst AS DOUBLE) AS pst,
            CAST(s.settlement_info AS STRING) AS settlement_info,
            CAST(s.revenue_job_id AS STRING) AS revenue_job_id,
            CAST(s.advance AS DOUBLE) AS advance,
            CAST(s.bdf AS DOUBLE) AS bdf,
            CAST(s.dst AS DOUBLE) AS dst,
            CAST(s.`at` AS DOUBLE) AS `at`,
            CAST(
                s.advertising_income AS DOUBLE
            ) AS advertising_income,
            CAST(
                s.unearned_advertising AS DOUBLE
            ) AS unearned_advertising,
            CAST(s.ar_adv AS DOUBLE) AS ar_adv,
            CAST(s.rf AS DOUBLE) AS rf,
            CAST(s.rft AS DOUBLE) AS rft,
            CAST(s.order_sn AS STRING) AS order_sn,
            CAST(s.rcr AS DECIMAL(34, 6)) AS rcr,
            CAST(s.business_scene AS STRING) AS business_scene,
            CAST(s.refund_amount AS DOUBLE) AS refund_amount,
            CAST(s.expect_date_local AS STRING) AS expect_date_local,
            CAST(
                s.rebate_liability AS DECIMAL(34, 6)
            ) AS rebate_liability,
            CAST(s.country AS STRING) AS country,
            CAST(s.month AS INT) AS month,
            s.deliverer_m_area_id,
            s.is_area_validated,
            CAST('dw_revenues' AS STRING) AS source,
            -- Generate IDs
            CONCAT_WS(
                '',
                s.country,
                CAST(s.deliverer_id AS STRING)
            ) AS d_id,
            CONCAT_WS(
                '',
                s.country,
                CAST(s.restaurant_id AS STRING)
            ) AS r_id,
            CONCAT_WS(
                '',
                s.country,
                CAST(s.order_id AS STRING)
            ) AS o_id,
            CONCAT_WS(
                '',
                s.country,
                CAST(s.wechat_id AS STRING)
            ) AS w_id,
            CASE
                WHEN s.restaurant_type = 'Restaurant'
                AND s.restaurant_id IS NOT NULL THEN CONCAT_WS(
                    '',
                    s.country,
                    CAST(s.restaurant_id AS STRING)
                )
                WHEN s.is_area_validated THEN CONCAT_WS(
                    '',
                    s.country,
                    'A',
                    CAST(
                        s.deliverer_m_area_id AS STRING
                    )
                )
                ELSE CONCAT_WS(
                    '',
                    s.country,
                    'AW',
                    CAST(s.wechat_id AS STRING)
                )
            END AS DR_id
        FROM deduped_normal_data s
    ),
    final_source_data AS (
        SELECT
            id,
            country,
            created_at_local,
            pr,
            updated_at,
            w_id,
            r_id,
            DR_id,
            desc_info,
            revenue - fc - td - tr - cd - cr - ra AS UE,
            - fc - ra AS B_adjustment,
            - cd - cr - td - tr AS D_adjusment,
            revenue AS C_adjustment,
            advertising_income,
            desc_info IN (
                'prime unrevenue',
                'prime revenue',
                '会员',
                'prime expect income'
            ) AS IsMember
        FROM final_revenues
        WHERE
            restaurant_status >= 0
            AND deliverer_status >= 0
            AND lower(desc_info) NOT LIKE '%pos%'
            AND desc_info NOT LIKE '%电动车%'
            AND (
                revenue - fc - td - tr - cd - cr - ra != 0
                OR advertising_income != 0
                OR pr != 0
            )
    )
select *
from final_source_data
limit 1000;