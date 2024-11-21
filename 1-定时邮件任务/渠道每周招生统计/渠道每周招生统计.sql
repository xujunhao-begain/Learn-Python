WITH order_info AS
(
    SELECT  a.order_no
            ,user_id
            ,NVL(c.ctime,a.pay_time) AS pay_time
            ,TO_DATE(NVL(c.ctime,a.pay_time)) AS pay_date
            ,a.subject_type
            ,channel_type_name
            ,channel_subtype_name
            ,a.channel_group_id
            ,enroll_channel_type
            ,a.package_grade
            ,CASE   WHEN enroll_channel_type = 'CPA' THEN '线上BD'
                    WHEN enroll_channel_type = 'BtoC' THEN 'BTC'
                    WHEN enroll_channel_type IN ('进校-春辉','进校-崔白') THEN '线上BD'
                    ELSE enroll_channel_type
            END AS target_mkt_group
            ,renewal_channel_type
            ,business_line_name
            ,business_mode_name
          ,CASE     WHEN b.channel_group_id = 15099 THEN '转介绍-召回'
                    WHEN b.business_mode_name = 'B2C公益课' THEN 'BTC'
                    WHEN b.business_mode_name = '测试' THEN '测试'
                    WHEN b.business_mode_name = '9.9元课' AND a.parent_operation_type_tag_name = '0元' THEN (
                            CASE WHEN a.renewal_channel_type = '进校合作' THEN '进校-4.5'
                            WHEN a.enroll_channel_type = 'CPA' AND a.channel_type_name = '短信' THEN '线上BD-短信-4.5'
                            WHEN a.enroll_channel_type = 'CPA' AND a.channel_type_name = '外呼' THEN '线上BD-外呼-4.5'
                            WHEN a.enroll_channel_type = 'CPA' AND a.channel_type_name = '信息流' THEN '线上BD-信息流-4.5'
                            WHEN a.enroll_channel_type = 'CPA' THEN '线上BD-其它-4.5'
                            ELSE '其它' END
                            )
                    WHEN b.business_mode_name = '9.9元课' THEN (
                                CASE    WHEN a.renewal_channel_type = '效果广告' AND a.channel_type_name = '搜索' THEN '效果广告-搜索'
                                        WHEN a.renewal_channel_type = '自孵化达人' THEN '直播-自孵化达人'
                                        WHEN a.renewal_channel_type = '效果广告' THEN '效果广告'
                                        WHEN a.renewal_channel_type = '直播投流' THEN '直播投流'
                                        WHEN a.enroll_channel_type = 'CPA' AND a.channel_type_name = '社群运营' THEN '线上BD商务-社群运营'
                                        WHEN a.enroll_channel_type = 'CPA' AND a.channel_type_name = '外呼' THEN '线上BD商务-外呼'
                                        WHEN a.renewal_channel_type IN ('线上BD商务','短信','召回') THEN '线上BD商务-其它'
                                        WHEN a.enroll_channel_type = '数字化营销' AND a.renewal_channel_type = '直播-kol' THEN '数字化营销-直播kol'
                                        WHEN a.enroll_channel_type = '内容营销' AND a.renewal_channel_type = '直播-kol' THEN '内容营销-直播kol'
                                        WHEN a.renewal_channel_type = '直播-kol' AND a.channel_subtype_name = '抖音' THEN '直播kol-抖音'
                                        WHEN a.renewal_channel_type = '直播-kol' AND a.channel_subtype_name <> '小风车-ad自营' THEN '直播kol-其它'
                                        WHEN a.renewal_channel_type = '转介绍' THEN '转介绍'
                                        WHEN a.renewal_channel_type = '进校合作' THEN '进校-9.9常规'
                                        WHEN a.renewal_channel_type = '线下地推' THEN '线下地推'
                                        WHEN a.enroll_channel_type = '自然量' THEN '自然量'
                                        ELSE '其它'
                                END
                    )
                    when b.business_mode_name = '0元课' then (
                            case when a.enroll_channel_type = '数字化营销' and a.channel_type_name = '应用商店' then '数字化营销-应用商店'
                            when a.renewal_channel_type = '进校合作' then '进校-0元常规'
                            when a.enroll_channel_type = 'CPA' AND a.channel_type_name = '外呼' THEN '线上BD-外呼-0元常规'
                            when a.enroll_channel_type = 'CPA' AND a.channel_type_name = '信息流' THEN '线上BD-信息流-0元常规'
                            when a.enroll_channel_type = 'CPA' then '线上BD-其它-0元常规'
                            when a.renewal_channel_type = '转介绍' then '转介绍-0元常规'
                            else '其它' end
                            )
                    ELSE '其它'
            END AS target_channel_type -- 定标渠道类型

            ,class_group_tag_name
            ,tar.is_target
            ,real_pay
            ,CONCAT(term_year,term_season) AS season
            ,parent_operation_type_tag_name
            ,operation_type_name
            ,operation_type_tag_name
            ,CASE   WHEN user_group_tag_name = 'KETE' then '科特' when user_group_tag_name = 'SIWEI' then '思维' else 'NA' end as user_group
            ,CASE   WHEN a.subject_type LIKE '%Python%' THEN 'Python'
                    WHEN operation_type_name = '硬件体验课' THEN 'Hardware'
                    WHEN a.subject_type IN ('Script','ScratchMorton') THEN '图形化'
                    ELSE a.subject_type
            END AS sku
            ,case when sub.renewal_counselor_group_name = '公益课组' then '公益课团队'
                  else concat(a.business_team_tag_name,'团队') end operation_group -- 承标团队
            ,a.term_id
            ,term_name
            ,REPLACE(operation_type_name,'涅槃-','') AS super_term_tag --统一学期标签
            ,CONCAT(term_year,term_name_type) AS term
            ,user_class_start_time
            ,term_start_time
            ,term_renewal_end_time
            ,is_renewal
            ,is_inrenewal_period
            ,IF(renewal_user_cnt IS NULL,0,renewal_user_cnt) AS renewal_user_cnt
            ,IF(lastday_renewal_user_cnt IS NULL,0,lastday_renewal_user_cnt) AS lastday_renewal_cnt
            ,is_renewal_non899 AS 2699re
            ,is_renewal899 AS 899re
            ,IF(renewal_gmv IS NULL,0,renewal_gmv) AS renewal_gmv
            ,CASE   WHEN pay_grade = 1 THEN '一年级'
                    WHEN pay_grade = 2 THEN '二年级'
                    WHEN pay_grade = 3 THEN '三年级'
                    WHEN pay_grade = 4 THEN '四年级'
                    WHEN pay_grade = 5 THEN '五年级'
                    WHEN pay_grade = 6 THEN '六年级'
                    WHEN pay_grade IN (7,8) THEN '幼儿'
                    WHEN pay_grade IN (11,12,13,14) THEN '初中'
                    ELSE '其它'
            END AS ub_pay_grade
            ,a.channel_group_name
    FROM    (
                SELECT  *
                        ,ROW_NUMBER() OVER (PARTITION BY user_id,term_id ORDER BY user_class_start_time DESC ) AS rk
                FROM    htcdm.dws_l1_order_term_learning_renewal_hdf
                WHERE   dt = REGEXP_REPLACE(DATE_SUB(current_date(),1),'-','')
                AND     SUBSTR(pay_time,1,7) >= '2024-01'
                AND     parent_operation_type_tag_name IN ('9.9','0元','BTC')
                AND     super_subject_id = 1
                AND     order_status != 'UNPAID'
            ) AS a
    LEFT JOIN   (
                    SELECT  channel_group_id
                            ,business_line_name
                            ,business_mode_name
                    FROM    htcdm.dim_channel_hdf
                    WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
                    AND     is_main_line = 1
                    GROUP BY channel_group_id
                             ,business_line_name
                             ,business_mode_name
                ) AS b
    ON      a.channel_group_id = b.channel_group_id
    LEFT JOIN   (
                    SELECT  order_number
                            ,ctime
                    FROM    htods.ods_main_third_party_order
                    WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
                    AND     order_number NOT IN ('159479253243581434908052','1611651860112419587949776')
                    AND     order_number IS NOT NULL
                ) AS c
    ON      a.order_no = c.order_number
    LEFT JOIN
            (
            SELECT  term_id
                    ,IF(gmv = 1,'标内学期','标外学期') AS is_target
            FROM    htods.ods_crm_enrollment_plan
            WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
            GROUP BY term_id
                    ,IF(gmv = 1,'标内学期','标外学期')
            ) AS tar
    ON      a.term_id = tar.term_id
    LEFT JOIN   (
                SELECT  class_id
                        ,subject_type
                        ,package_grade
                        ,renewal_counselor_group_name
                FROM    htcdm.dim_class_hdf
                WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
                GROUP BY class_id
                        ,subject_type
                        ,package_grade
                        ,renewal_counselor_group_name
            ) AS sub
    ON      a.user_class_group_id = sub.class_id
    WHERE   a.rk = 1
)
,total_stock AS
(
    SELECT  order_no
            ,stock_id
            ,total_stock
            ,CASE   WHEN stock_status = 0 THEN '上架'
                    WHEN stock_status = 1 THEN '下架'
                    WHEN stock_status = 2 THEN '删除'
            END AS stock_status
    FROM    htcdm.dws_trade_order_stat_hdf
    WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
)
,enrollment_count AS
(
    SELECT  a.enrollment_plan_id
            ,a.term_id
            ,b.enrollment_count
            ,b.max_distribute_user_count
            ,b.opened
            ,b.deleted
            ,b.auto_distribute_opend
    FROM    (
                SELECT  enrollment_plan_id
                        ,term_id
                FROM    htcdm.dwd_consum_enrollment_plan_class_hdf
                WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
                AND     term_id > 0
                GROUP BY enrollment_plan_id
                         ,term_id
            ) AS a
    LEFT JOIN   (
                    SELECT  *
                    FROM    (
                                SELECT  enrollment_plan_id
                                        ,enrollment_count
                                        ,max_distribute_user_count
                                        ,opened
                                        ,deleted
                                        ,auto_distribute_opend
                                        ,ROW_NUMBER() OVER (PARTITION BY enrollment_plan_id ORDER BY utime DESC ) AS rn
                                FROM    htods.ods_crm_enrollment_pool
                                WHERE   dt = REGEXP_REPLACE(DATE_ADD(CURRENT_TIMESTAMP,-1),'-','')
                                AND     deleted = 0
                            ) AS aa
                    WHERE   rn = 1
                ) AS b
    ON      a.enrollment_plan_id = b.enrollment_plan_id
    WHERE   b.enrollment_count IS NOT NULL
)
,result AS
(
    SELECT  oi.*
            ,b.stock_id
            ,b.total_stock
            ,b.stock_status
            ,c.enrollment_count
    FROM    order_info AS oi
    LEFT JOIN total_stock AS b
    ON      oi.order_no = b.order_no
    LEFT JOIN enrollment_count AS c
    ON      oi.term_id = c.term_id
)

SELECT  user_group AS `用户群体`
        ,sku AS SKU
        ,target_mkt_group AS `业务核算组`
        ,channel_subtype_name AS `渠道子类型`
        ,target_channel_type AS `定标渠道类型`
        ,if(user_group='科特',channel_group_name,null) AS `渠道组名称`
        ,parent_operation_type_tag_name AS `业务线`
        ,operation_type_tag_name AS `班型`
        ,package_grade AS `年级`
        ,ub_pay_grade AS `支付年级`
        ,pay_date AS `支付日期`
        ,is_target AS `是否标内学期`
        ,operation_group AS `运营团队`
        ,term_id AS `学期id`
        ,enrollment_count AS `学期招生池承接人数`
        ,stock_id AS `库存id`
        ,total_stock AS `总库存`
        ,stock_status AS `库存状态`
        ,term AS `学期期次`
        ,TO_DATE(term_start_time) AS `学期开课时间`
        ,TO_DATE(term_renewal_end_time) AS `学期结束续报时间`
        ,IF(TO_DATE(term_renewal_end_time) >= '2024-01-01'
        AND     TO_DATE(term_renewal_end_time) < current_date(),'已结束','未结束') AS `学期是否结束续报`
        ,COUNT(DISTINCT user_id) AS `进班人数`
        ,COUNT(DISTINCT order_no) AS `订单数`
        ,SUM(lastday_renewal_cnt) AS `末日续报用户数` --计算续报率
        ,SUM(renewal_user_cnt) AS `累计续报用户数` --累计
FROM    result
WHERE   (
            term LIKE '2024春%'
            OR      term LIKE '2024暑%'
            OR      term LIKE '2024秋%'
            OR      term LIKE '2025寒%'
)
GROUP BY user_group
         ,sku
         ,target_mkt_group
         ,package_grade
         ,ub_pay_grade
         ,pay_date
         ,channel_subtype_name
         ,target_channel_type
         ,if(user_group='科特',channel_group_name,null)
         ,parent_operation_type_tag_name
         ,operation_type_tag_name
         ,is_target
         ,operation_group
         ,term_id
         ,enrollment_count
         ,stock_id
         ,total_stock
         ,stock_status
         ,term
         ,TO_DATE(term_start_time)
         ,TO_DATE(term_renewal_end_time)
         ,IF(TO_DATE(term_renewal_end_time) >= '2024-01-01'
         AND     TO_DATE(term_renewal_end_time) < current_date(),'已结束','未结束')
         ,CURRENT_DATE