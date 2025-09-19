{{ config(materialized='table') }}

-- Purpose (my words):
-- I want a theme-level portfolio view: how many sets, the lifespan (first/last year),
-- total and average parts, average color diversity per set, and average minifigs per set.
-- This helps me compare themes, prioritize content, and summarize the catalog for execs.

-- I reuse the per-set composition table to avoid redoing BOM math.
with set_comp as (
  select *
  from {{ ref('sets_composition_metrics') }}
),

-- I compute minifigs per set via inventories → inventory_minifigs.
inventories as (
  select
    inventory_id,
    set_id
  from {{ source('lego_db', 'inventories') }}
),
inventory_minifigs as (
  select
    inventory_id,
    fig_id,
    quantity
  from {{ source('lego_db', 'inventory_minifigs') }}
),
minifigs_per_set as (
  select
    i.set_id,
    sum(coalesce(im.quantity, 0)) as minifig_count
  from inventories i
  left join inventory_minifigs im on im.inventory_id = i.inventory_id
  group by i.set_id
)

-- Final theme portfolio KPIs
select
  sc.theme_name,

  -- Breadth and lifespan
  count(distinct sc.set_id)                      as sets_count,
  min(sc.year)                                   as first_year,
  max(sc.year)                                   as last_year,

  -- Parts volume and complexity proxies
  sum(coalesce(sc.total_parts_from_bom, 0))      as total_parts,
  avg(coalesce(sc.total_parts_from_bom, 0))      as avg_parts_per_set,
  avg(coalesce(sc.distinct_colors, 0))           as avg_colors_per_set,

  -- Minifig density
  avg(coalesce(mps.minifig_count, 0))            as avg_minifigs_per_set,
  sum(coalesce(mps.minifig_count, 0))            as total_minifigs
from set_comp sc
left join minifigs_per_set mps on mps.set_id = sc.set_id
group by sc.theme_name
order by sets_count desc
