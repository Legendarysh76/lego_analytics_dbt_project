{{ config(
    materialized='table'
) }}

WITH sets AS (

    SELECT set_id,
    set_name,
    year,
    num_of_parts,
    theme_id
   FROM {{ source('lego_db', 'sets') }}
),

themes AS (
    SELECT
        theme_id,
        theme_name
    FROM {{ source('lego_db', 'themes') }}
)

SELECT
    s.set_id,
    s.set_name,
    s.year,
    s.num_of_parts,
    t.theme_name
FROM sets s
LEFT JOIN themes t ON s.theme_id = t.theme_id