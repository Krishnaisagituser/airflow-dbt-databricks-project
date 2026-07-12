{{config (severity= 'warn')}}

SELECT 1
FROM
{{ref("obt_b")}} as obt
WHERE
    obt.order_id IS NULL 
    or obt.order_item_id IS NULL
    or obt.product_id IS NULL
    or obt.customer_id IS NULL
    or obt.store_id IS NULL
    or obt.employee_id IS NULL