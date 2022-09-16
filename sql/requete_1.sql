-- 1er partie

SELECT
 order_cmd.date as order_date,
 SUM(order_cmd.prod_qty * order_cmd.prod_price) as ca
FROM TRANSACTION as order_cmd
WHERE CAST(order_cmd.date as DATE FORMAT'DD/MM/YYYY') between CAST('01/01/2020' as DATE FORMAT'DD/MM/YYYY') and CAST('31/12/2020' as DATE FORMAT'DD/MM/YYYY')
GROUP BY order_cmd.date 
ORDER BY order_cmd.date asc;  