{{ config(materialized='table') }}

-- Purpose (my words):
-- I want a per-set “richness” profile that compares the catalog parts count
-- to the actual bill of materials (BOM) from inventories. I also summarize
-- complexity signals (distinct parts/colors), spare parts, and the dominant
-- color and part category by quantity. This helps me spot data mismatches,
-- highlight complex sets, and support merchandising decisions.

-- Prereqs:
-- - Sources are the cleaned tables in lego_db (defined in lego_sources.yml):
--   sets, themes, inventories, inventory_parts, parts, part_categories, colors.

with
-- Base sources
sets as (
  select
    set_id,
    set_name,
    year,
    theme_id,
    num_of_parts as provided_num_of_parts
  from {{ source('lego_db', 'sets') }}
),
themes as (
  select
    theme_id,
    theme_name
  from {{ source('lego_db', 'themes') }}
),
inventories as (
  select
    inventory_id,
    set_id
  from {{ source('lego_db', 'inventories') }}
),
inventory_parts as (
  select
    inventory_id,
    part_id,
    color_id,
    quantity,
    is_spare_part
  from {{ source('lego_db', 'inventory_parts') }}
),
parts as (
  select
    part_id,
    part_category_id
  from {{ source('lego_db', 'parts') }}
),
part_categories as (
  select
    part_category_id,
    part_category_name
  from {{ source('lego_db', 'part_categories') }}
),
colors as (
  select
    color_id,
    color_name
  from {{ source('lego_db', 'colors') }}
),

-- I normalize spare flags and enrich inventory parts with category and color context.
ip_enriched as (
  select
    i.set_id,
    ip.part_id,
    ip.color_id,
    coalesce(ip.quantity, 0) as quantity,
    case
      when lower(trim(cast(ip.is_spare_part as string))) in ('y','yes','true','1','t') then 1
      else 0
    end as is_spare_flag
  from inventories i
  join inventory_parts ip on ip.inventory_id = i.inventory_id
),

-- I attach part categories for category-level rollups.
ip_with_category as (
  select
    e.set_id,
    e.part_id,
    e.color_id,
    e.quantity,
    e.is_spare_flag,
    pc.part_category_name
  from ip_enriched e
  left join parts p on p.part_id = e.part_id
  left join part_categories pc on pc.part_category_id = p.part_category_id
),

-- I compute set-level aggregates: total BOM qty, spare qty, distinct parts/colors.
set_level_agg as (
  select
    set_id,
    sum(quantity)                                              as total_parts_from_bom,
    sum(case when is_spare_flag = 1 then quantity else 0 end) as spare_parts_qty,
    count(distinct part_id)                                    as distinct_parts,
    count(distinct color_id)                                   as distinct_colors
  from ip_with_category
  group by set_id
),

-- I find the dominant color per set by BOM quantity.
ranked_colors as (
  select
    set_id,
    color_id,
    sum(quantity) as qty,
    row_number() over (partition by set_id order by sum(quantity) desc) as rn
  from ip_with_category
  group by set_id, color_id
),
top_color as (
  select
    rc.set_id,
    c.color_name as top_color_name,
    rc.qty       as top_color_qty
  from ranked_colors rc
  left join colors c on c.color_id = rc.color_id
  where rc.rn = 1
),

-- I find the dominant part category per set by BOM quantity.
ranked_categories as (
  select
    set_id,
    part_category_name,
    sum(quantity) as qty,
    row_number() over (partition by set_id order by sum(quantity) desc) as rn
  from ip_with_category
  group by set_id, part_category_name
),
top_category as (
  select
    set_id,
    part_category_name as top_part_category,
    qty               as top_part_category_qty
  from ranked_categories
  where rn = 1
)

-- Final per-set composition view
select
  s.set_id,
  s.set_name,
  s.year,
  t.theme_name,

  -- Provided vs computed parts (I keep the delta to spot mismatches)
  s.provided_num_of_parts,
  coalesce(a.total_parts_from_bom, 0)                       as total_parts_from_bom,
  coalesce(a.total_parts_from_bom, 0) - s.provided_num_of_parts as parts_count_delta,

  -- Richness signals
  coalesce(a.distinct_parts, 0)                             as distinct_parts,
  coalesce(a.distinct_colors, 0)                            as distinct_colors,
  coalesce(a.spare_parts_qty, 0)                            as spare_parts_qty,

  -- Descriptive leaders
  tc.top_color_name,
  coalesce(tc.top_color_qty, 0)                             as top_color_qty,
  cat.top_part_category,
  coalesce(cat.top_part_category_qty, 0)                    as top_part_category_qty
from sets s
left join themes t on t.theme_id = s.theme_id
left join set_level_agg a on a.set_id = s.set_id
left join top_color tc on tc.set_id = s.set_id
left join top_category cat on cat.set_id = s.set_id

