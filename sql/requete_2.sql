-- 2n partie

SELECT
 order_cmd.client_id,
 sum(case when product.product_type = 'MEUBLE' then (order_cmd.prod_price * order_cmd.prod_qty)
	  else 0 
 end) as ventes_meuble,
 sum(case when product.product_type = 'DECO' then (order_cmd.prod_price * order_cmd.prod_qty)
	  else 0 
 end) as ventes_deco
FROM TRANSACTION as order_cmd
LEFT JOIN PRODUCT_NOMENCLATURE product
	ON order_cmd.prop_id = product.product_id
WHERE CAST(order_cmd.date as DATE FORMAT'DD/MM/YYYY') between CAST('01/01/2020' as DATE FORMAT'DD/MM/YYYY') and CAST('31/12/2020' as DATE FORMAT'DD/MM/YYYY')
GROUP BY order_cmd.client_id