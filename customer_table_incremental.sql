{{ config(
    materialized='incremental',
    unique_key='customer_id',
    target_schema='test',
    strategy='timestamp',
    updated_at='last_update_time'
) }}

WITH source_data AS (
  SELECT
    BuyerInfo_BuyerEmail AS customer_id,
    PurchaseDate,
    LastUpdateDate,
    OrderStatus,
    ShipServiceLevel,
    NumberOfItemsShipped,
    NumberOfItemsUnshipped,
    _daton_batch_runtime, 
  FROM `hallowed-garden-418610.test.Listordersdetails`
  WHERE _daton_batch_runtime > (SELECT MAX(_daton_batch_runtime) FROM hallowed-garden-418610.test.customer_table)
),

customer_aggregations AS (
  SELECT
    customer_id,
    MIN(PurchaseDate) AS customer_acquisition_date,
    MAX(CASE WHEN OrderStatus = 'Shipped' THEN LastUpdateDate END) AS customer_last_order_date,
    OrderStatus,
    ShipServiceLevel,
  FROM source_data
  GROUP BY 1, 4, 5
),

order_status_counts AS (
  SELECT
    OrderStatus,
    COUNT(*) AS orderstatus_count
  FROM source_data
  GROUP BY OrderStatus
),

Shipment_Service_Level_status_counts AS (
  SELECT
    ShipServiceLevel,
    COUNT(*) AS ShipServiceLevel_count
  FROM source_data
  GROUP BY ShipServiceLevel
)

SELECT
  c.customer_id,
  c.customer_acquisition_date,
  c.customer_last_order_date,
  c.OrderStatus,
  o.orderStatus_count,
  c.ShipServiceLevel,
  s.ShipServiceLevel_count
FROM customer_aggregations c
JOIN order_status_counts o ON c.OrderStatus = o.OrderStatus
JOIN Shipment_Service_Level_status_counts s ON c.ShipServiceLevel = s.ShipServiceLevel
