select sku, warehouse, on_hand, reserved, updated_ts
from {{ ref('stg_inventory') }}
qualify row_number() over (partition by sku, warehouse order by updated_ts desc) = 1
