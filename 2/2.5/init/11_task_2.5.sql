CREATE OR REPLACE VIEW public."task_2.5"
AS WITH fct_tbl AS (
         SELECT shop_dns.shop_id AS shop,
            shop_dns.date,
            shop_dns.product_id,
            shop_dns.sales_cnt
           FROM shop_dns
        UNION ALL
         SELECT shop_mvideo.shop_id,
            shop_mvideo.date,
            shop_mvideo.product_id,
            shop_mvideo.sales_cnt
           FROM shop_mvideo
        UNION ALL
         SELECT shop_sitilink.shop_id,
            shop_sitilink.date,
            shop_sitilink.product_id,
            shop_sitilink.sales_cnt
           FROM shop_sitilink
  ORDER BY 2
        ), 
        
        fct_tbl_agg AS (
         SELECT fct_tbl.shop,
            (date_trunc('MONTH'::text, fct_tbl.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date AS last_day,
            fct_tbl.product_id,
            sum(fct_tbl.sales_cnt) AS fct_cnt
           FROM fct_tbl
          GROUP BY fct_tbl.shop, ((date_trunc('MONTH'::text, fct_tbl.date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date), fct_tbl.product_id
        )
        
 SELECT to_char(p.plan_date::timestamp with time zone, 'Mon-YYYY'::text) AS "Месяц продаж",
    sh.shop_name,
    pr.product_name,
    f.fct_cnt AS sales_fact,
    p.plan_cnt AS sales_plan,
    round(f.fct_cnt::numeric / p.plan_cnt::numeric, 2) AS "sales_fact/sales_plan",
    f.fct_cnt::numeric * pr.price AS income_fact,
    p.plan_cnt::numeric * pr.price AS income_plan,
    round((f.fct_cnt::numeric * pr.price) / (p.plan_cnt::numeric * pr.price), 2) AS "income_fact/income_plan"
   FROM plan p
     LEFT JOIN fct_tbl_agg f ON p.shop_id = f.shop AND p.plan_date = f.last_day AND p.product_id = f.product_id
     LEFT JOIN products pr ON p.product_id = pr.product_id
     LEFT JOIN shops sh ON p.shop_id = sh.shop_id;