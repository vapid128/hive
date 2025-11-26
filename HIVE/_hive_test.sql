select distinct
    gme.merchant_id
from dw.dw_group_merchant_extra gme
    left join dw.dw_dianping_merchant_bing_category mbc on gme.review_merchant_id = mbc.merchant_id
where
    mbc.bing_category like '1-9-1%'