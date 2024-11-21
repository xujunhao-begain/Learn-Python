WITH order_info AS
(
    SELECT  a.order_no
            ,user_id
            ,NVL(c.ctime,a.pay_time) AS pay_time
            ,TO_DATE(NVL(c.ctime,a.pay_time)) AS pay_date
            ,subject_type
            ,channel_type_name
            ,channel_subtype_name
            ,a.channel_group_id
            ,enroll_channel_type
            ,package_grade
            ,CASE   WHEN enroll_channel_type = 'CPA' THEN '线上BD'
                    WHEN enroll_channel_type = 'BtoC' THEN 'BTC'
                    WHEN enroll_channel_type IN ('进校-春辉','进校-崔白') THEN '线上BD'
                    ELSE enroll_channel_type
            END AS target_mkt_group
            ,renewal_channel_type
            ,business_line_name
            ,business_mode_name
            ,CASE   WHEN b.business_mode_name = 'B2C公益课' THEN 'BTC'
                    WHEN b.business_mode_name = '测试' THEN '测试'
                    WHEN b.business_mode_name = '9.9元课'
                        AND parent_operation_type_name = '0元'
                        AND renewal_channel_type IN ('线上BD商务','短信','召回') THEN '线上BD商务-4.5'
                    WHEN b.business_mode_name = '9.9元课'
                        AND parent_operation_type_name = '0元'
                        AND renewal_channel_type = '进校合作' THEN '进校-4.5'
                    WHEN b.business_mode_name = '9.9元课' THEN (CASE   WHEN renewal_channel_type = '自孵化达人' THEN '直播-自孵化达人'
                            WHEN renewal_channel_type = '效果广告' THEN '效果广告'
                            WHEN renewal_channel_type = '直播投流' THEN '直播投流'
                            WHEN enroll_channel_type = 'CPA' AND channel_type_name = '外呼' THEN '线上BD商务-外呼'
                            WHEN renewal_channel_type IN ('线上BD商务','短信','召回') THEN '线上BD商务-其它'
                            WHEN enroll_channel_type = '数字化营销' AND renewal_channel_type = '直播-kol' THEN '数字化营销-直播kol'
                            WHEN enroll_channel_type = '内容营销' AND renewal_channel_type = '直播-kol' THEN '内容营销-直播kol'
                            WHEN renewal_channel_type = '直播-kol' AND channel_subtype_name = '抖音' THEN '直播kol-抖音'
                            WHEN renewal_channel_type = '直播-kol' AND channel_subtype_name <> '小风车-ad自营' THEN '直播kol-其它'
                            WHEN renewal_channel_type = '转介绍' THEN '转介绍'
                            WHEN renewal_channel_type = '进校合作' THEN '进校-9.9常规'
                            WHEN renewal_channel_type = '线下地推' THEN '线下地推'
                            WHEN enroll_channel_type = '自然量' THEN '自然量'
                            ELSE '其它'
                    END)
                    WHEN b.business_mode_name = '0元课' THEN (CASE   WHEN enroll_channel_type = '数字化营销' THEN '数字化营销-0元' -- 这里不一定是效果广告，可能也是一些CPA类型的进量
                            WHEN renewal_channel_type IN ('线上BD商务','短信','召回') THEN '线上BD商务-0元常规'
                            WHEN renewal_channel_type = '进校合作' THEN '进校-0元常规'
                            WHEN renewal_channel_type = '转介绍' THEN '转介绍'
                            ELSE '其它'
                    END)
                    ELSE '其它'
            END AS target_channel_type
            ,class_group_tag_name
            ,IF(operation_type_name IN ('硬件体验课','蜀都新兵营','科特报告加购测试') OR class_group_tag_name = '召回测试','标外学期','标内学期') AS is_target --判断标外
            ,real_pay
            ,CONCAT(term_year,term_season) AS season
            ,parent_operation_type_name
            ,operation_type_name
            ,CASE   WHEN user_group_tag_name = 'KETE' then '科特' when user_group_tag_name = 'SIWEI' then '思维' else 'NA' end as user_group
            ,CASE   WHEN subject_type LIKE '%Python%' THEN 'Python'
                    WHEN operation_type_name = '硬件体验课' THEN 'Hardware'
                    WHEN subject_type IN ('Script','ScratchMorton') THEN '图形化'
                    ELSE subject_type
            END AS sku
            ,CASE   WHEN class_group_tag_name = '精细化运营-成都' OR operation_type_name = '蜀都新兵营' THEN '蜀都团队'
                    WHEN parent_operation_type_name IN ('9.9','BTC') THEN '9元团队'
                    WHEN parent_operation_type_name = '0元' THEN '0元团队'
                    ELSE 'UNKNOWN'
            END AS operation_group --判断团队
            ,term_id
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
            ,a.counselor_group_city
            ,a.counselor_group_name
    FROM    (
                SELECT  *
                        ,ROW_NUMBER() OVER (PARTITION BY user_id,term_id ORDER BY user_class_start_time DESC ) AS rk
                FROM    htcdm.dws_l1_order_term_learning_renewal_hdf
                WHERE   dt = REGEXP_REPLACE(DATE_SUB(current_date(),1),'-','')
                AND     SUBSTR(pay_time,1,7) >= '2024-01'
                AND     parent_operation_type_name IN ('9.9','0元','BTC')
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
SELECT  user_group
        ,sku
        ,target_mkt_group
        ,channel_subtype_name
        ,target_channel_type
        ,parent_operation_type_name
        ,CASE   WHEN super_term_tag = 'Python常规'
                    AND (class_group_tag_name IN ('常规班')
                    OR class_group_tag_name IS NULL) THEN '常规学期'
                WHEN super_term_tag = 'Python常规' AND class_group_tag_name = '直播渠道' THEN '直播单独运营'
                WHEN super_term_tag = 'Python常规' THEN class_group_tag_name
                WHEN user_group = '科特'
                    AND sku = '图形化'
                    AND super_term_tag = '科特单独运营' THEN '直播单独运营'
                WHEN user_group = '科特'
                    AND sku = 'Python'
                    AND super_term_tag = 'Python科特' THEN '直播单独运营'
                WHEN super_term_tag = '0元新兵营-正式' THEN '常规学期'
                ELSE super_term_tag
        END AS class_tag --判断班型
        ,package_grade
        ,pay_date
        ,is_target
        ,operation_group
        ,counselor_group_city
        ,term_id
        ,enrollment_count AS term_enrollment_count
        ,stock_id
        ,total_stock
        ,stock_status
        ,term
        ,TO_DATE(term_start_time) AS term_start_date
        ,TO_DATE(term_renewal_end_time) AS term_renewal_end_date
        ,IF(TO_DATE(term_renewal_end_time) >= '2024-01-01'
        AND     TO_DATE(term_renewal_end_time) < current_date(),'已结束','未结束') AS is_term_end_renewal
        ,COUNT(DISTINCT user_id) AS user_cnt
        ,COUNT(DISTINCT order_no) AS order_cnt
        ,SUM(lastday_renewal_cnt) AS lastday_renewal_cnt
        ,SUM(renewal_user_cnt) AS renewal_user_cnt
        ,CURRENT_DATE AS modify_date
FROM    result
WHERE   term LIKE '2024暑%'
OR      term LIKE '2024秋%'
OR      term LIKE '2025寒%'
GROUP BY user_group
         ,sku
         ,target_mkt_group
         ,package_grade
         ,pay_date
         ,channel_subtype_name
         ,target_channel_type
         ,parent_operation_type_name
         ,CASE   WHEN super_term_tag = 'Python常规'
                     AND (
                             class_group_tag_name IN ('常规班')
                                 OR class_group_tag_name IS NULL
                 ) THEN '常规学期'
                 WHEN super_term_tag = 'Python常规' AND class_group_tag_name = '直播渠道' THEN '直播单独运营'
                 WHEN super_term_tag = 'Python常规' THEN class_group_tag_name
                 WHEN user_group = '科特'
                     AND sku = '图形化'
                     AND super_term_tag = '科特单独运营' THEN '直播单独运营'
                 WHEN user_group = '科特'
                     AND sku = 'Python'
                     AND super_term_tag = 'Python科特' THEN '直播单独运营'
                 WHEN super_term_tag = '0元新兵营-正式' THEN '常规学期'
                 ELSE super_term_tag
         END
         ,is_target
         ,operation_group
         ,counselor_group_city
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