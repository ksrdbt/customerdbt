{{ config(
  materialized='table',
  unique_key='customer_id',
  schema='test' 
) }}

with source_data as (
  select
    BuyerInfo_BuyerEmail as customer_id,
    PurchaseDate,
    OrderStatus,
    ShipmentServiceLevelCategory,
    NumberOfItemsShipped,
    NumberOfItemsUnshipped
  from hallowed-garden-418610.test.ListOrderingTable
),

customer_aggregations as (
  select
    customer_id,
    min(PurchaseDate) as customer_acquisition_date,
    max(case when OrderStatus = 'Success' then PurchaseDate end) as customer_last_order_date,
    count(case when NumberOfItemsShipped > 0 then 1 end) as num_orders_shipped,
    count(case when NumberOfItemsUnshipped > 0 then 1 end) as num_orders_unshipped,
    count(*) as total_orders,
    count(distinct ShipmentServiceLevelCategory) as num_shipment_service_levels
  from source_data
  group by customer_id
)

select
  customer_id,
  customer_acquisition_date,
  customer_last_order_date,
  num_orders_shipped,
  num_orders_unshipped,
  total_orders,
  num_shipment_service_levels
from customer_aggregations
