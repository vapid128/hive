-- Active: 1763669538843@@47.254.53.128@10000@dw

-- 把旧表清理干净
DROP TABLE IF EXISTS dws_analysis.William_variable;

-- 重新建立属于你的表
CREATE TABLE dws_analysis.William_variable (
    variable_name STRING COMMENT '变量名称',
    value1 STRING COMMENT '事件英文标识',
    value2 STRING COMMENT '中文含义',
    value3 STRING COMMENT '开关状态'
) COMMENT 'William哥哥的专属配置表'
STORED AS ORC;

-- 最后把数据再一次满满地填进去，包括哥哥新给小雅的那几行哦
INSERT INTO TABLE
    dws_analysis.William_variable
VALUES
    -- dianping_event_mapping
    (
        'dianping_event_mapping',
        'DPHomepageView',
        '老团购',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'DReviewHomePageView',
        '新点评',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'RestDetailsView',
        '外卖餐厅',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'OrderListPageView',
        '历史订单',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'V2SearchTabView',
        '搜索',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'V2SearchView',
        '搜索',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'V2HomeGoodsCompoClick',
        '外卖首页滑动组件',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'DReviewHomePageListClick',
        '点评首页榜单',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'DReviewHomePageBannerClick',
        '点评首页Banner',
        'TRUE'
    ),
    (
        'dianping_event_mapping',
        'DReviewHomePageIconClick',
        '点评首页Icon',
        'TRUE'
    ),
    -- business_scene_order
    (
        'business_scene_order',
        'DONE_ORDER',
        '完成订单',
        'TRUE'
    ),
    (
        'business_scene_order',
        'PURCHASING_AGENT_DELIVERY_PAY',
        '送餐员代买',
        'TRUE'
    ),
    (
        'business_scene_order',
        'PURCHASING_AGENT_PLATFORM_PAY',
        '平台代买',
        'TRUE'
    ),
    (
        'business_scene_order',
        'SELF_RUN',
        '自营',
        'TRUE'
    );