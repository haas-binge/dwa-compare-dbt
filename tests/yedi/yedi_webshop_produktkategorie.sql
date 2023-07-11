
{{ config( enabled=True) }}


WITH
cte_load_date as
(
  SELECT file_ldts as ldts
  FROM {{ ref("meta_load") }} meta_load
  WHERE table_name = 'load_webshop_produktkategorie'
  qualify max(ldts) OVER (PARTITION BY TABLE_NAME) = ldts
),
cte_load AS
(
    SELECT
        katid
    , oberkatid
    , name
        , ldts
    FROM {{ ref("load_webshop_produktkategorie") }} load_webshop_produktkategorie
    where is_check_ok
)
, cte_productcategory as
(
    with cte_productcategory_h as
    (
        SELECT
            productcategory_h.hk_productcategory_h, IFF(productcategory_bk != '(unknown)', productcategory_bk, NULL) as katid
        FROM {{ ref("productcategory_h") }}  productcategory_h
    )
    ,cte_productcategory_ws_s as
    (
        SELECT    
              cte_productcategory_h.hk_productcategory_h
            , name, productcategory_ws_s.ldts, COALESCE(LEAD(productcategory_ws_s.ldts - INTERVAL '1 MICROSECOND') OVER (PARTITION BY cte_productcategory_h.hk_productcategory_h  ORDER BY productcategory_ws_s.ldts),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) as ledts
        FROM cte_productcategory_h
        INNER JOIN {{ ref("productcategory_ws_s") }} productcategory_ws_s
            ON cte_productcategory_h.hk_productcategory_h = productcategory_ws_s.hk_productcategory_h
        WHERE productcategory_ws_s.hk_productcategory_h <> '00000000000000000000000000000000'
    ) 
    SELECT  
        cte_productcategory_h.hk_productcategory_h
        , cte_productcategory_h.katid
        , cte_productcategory_ws_s.name
        , d.ldts
    FROM cte_load_date d
    CROSS JOIN cte_productcategory_h
    INNER JOIN  cte_productcategory_ws_s
        ON cte_productcategory_ws_s.hk_productcategory_h = cte_productcategory_h.hk_productcategory_h
        AND d.ldts between cte_productcategory_ws_s.ldts AND cte_productcategory_ws_s.ledts
)
        
, cte_productcategory_hierarchy as
(
    with cte_productcategory_hierarchy_l as
    (
        SELECT
            productcategory_hierarchy_l.hk_productcategory_hierarchy_l, hk_productcategory_h, hk_productcategory_parent_h
        FROM {{ ref("productcategory_hierarchy_l") }} productcategory_hierarchy_l
    )
    ,cte_productcategory_hierarchy_ws_sts as
    (SELECT * FROM 
        (
        SELECT    
              cte_productcategory_hierarchy_l.hk_productcategory_hierarchy_l, productcategory_hierarchy_ws_sts.ldts, productcategory_hierarchy_ws_sts.cdc, COALESCE(LEAD(productcategory_hierarchy_ws_sts.ldts - INTERVAL '1 MICROSECOND') OVER (PARTITION BY cte_productcategory_hierarchy_l.hk_productcategory_hierarchy_l  ORDER BY productcategory_hierarchy_ws_sts.ldts),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) as ledts
        FROM cte_productcategory_hierarchy_l
        INNER JOIN {{ ref("productcategory_hierarchy_ws_sts") }} productcategory_hierarchy_ws_sts
            ON cte_productcategory_hierarchy_l.hk_productcategory_hierarchy_l = productcategory_hierarchy_ws_sts.hk_productcategory_hierarchy_l
        )
        WHERE cdc <> 'D'
    ) 
    SELECT  
        cte_productcategory_hierarchy_l.hk_productcategory_hierarchy_l
        , cte_productcategory_hierarchy_l.hk_productcategory_h
        , cte_productcategory_hierarchy_l.hk_productcategory_parent_h
        , d.ldts
    FROM cte_load_date d
    CROSS JOIN cte_productcategory_hierarchy_l
    INNER JOIN  cte_productcategory_hierarchy_ws_sts
        ON cte_productcategory_hierarchy_ws_sts.hk_productcategory_hierarchy_l = cte_productcategory_hierarchy_l.hk_productcategory_hierarchy_l
        AND d.ldts between cte_productcategory_hierarchy_ws_sts.ldts AND cte_productcategory_hierarchy_ws_sts.ledts
)
,
cte_target as
(   
    SELECT
    cte_productcategory.katid
    , cte_productcategory.name
    , cte_productcategory_parent.katid as oberkatid
    , cte_load_date.ldts
    FROM cte_load_date 
    INNER JOIN cte_productcategory_hierarchy
        ON cte_productcategory_hierarchy.ldts = cte_load_date.ldts
    INNER JOIN  cte_productcategory
        ON cte_productcategory_hierarchy.hk_productcategory_h = cte_productcategory.hk_productcategory_h
        AND cte_productcategory.ldts =  cte_load_date.ldts
    left JOIN  cte_productcategory as cte_productcategory_parent
        ON cte_productcategory_hierarchy.hk_productcategory_parent_h = cte_productcategory_parent.hk_productcategory_h
        AND cte_productcategory_parent.ldts =  cte_load_date.ldts
)
(
    select
    katid,
    oberkatid
            , name
            , ldts
    from cte_load
    MINUS
    select
        katid
    , oberkatid
    , name
    , ldts
    from cte_target
)    
UNION
(
    select
        katid
        , oberkatid
    , name
            , ldts
    from cte_target
    minus
    select
        katid
    , oberkatid
    , name
    , ldts
    from cte_load
)
