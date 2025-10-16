select sku, title, category, cost, price
from {{ ref('stg_products') }}
