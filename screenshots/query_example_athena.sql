## The top two revenues in Canada and the US and your order date

WITH metrics as (
    select 
        country,
        order_date,
        total_revenue,
        row_number() over (partition by country order by total_revenue desc) as R
    from data_deliveryzone where country in ('Canada', 'United States of America')
)
SELECT
    country,
    order_date,
    total_revenue
FROM
    metrics
where R <=2    
order by 3 desc;