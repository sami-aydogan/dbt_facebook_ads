{{ config(
    materialized="ephemeral"
)}}

with impression_devices as (
    select 'desktop' as impression_device, 'desktop' as impression_device_category
    union all
    select 'other' AS impression_device, 'mobile' as impression_device_category
)

select * from impression_devices