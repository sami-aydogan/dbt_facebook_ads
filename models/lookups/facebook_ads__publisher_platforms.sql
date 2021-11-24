{{ config(
    materialized="ephemeral"
)}}

with impression_devices as (
    select 'instagram' as publisher_platform, 'ig' as site_source_name_value

    union all

    select 'facebook' as publisher_platform, 'fb' as site_source_name_value

    union all

    select 'audience_network' as publisher_platform, 'an' as site_source_name_value

    union all

    select 'messenger' as publisher_platform, 'msg' as site_source_name_value
)

select * from impression_devices