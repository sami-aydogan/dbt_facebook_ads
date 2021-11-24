with report as (

    select *
    from {{ var('client_performance_ads') }}

), creatives as (

    select *
    from {{ ref('facebook_ads__creative_history_prep') }}

), impression_devices as (

    select *
    from {{ ref('facebook_ads__impression_devices') }}

), placement_values as (

    select *
    from {{ ref('facebook_ads__placement_values') }}

), publisher_platforms as (

    select *
    from {{ ref('facebook_ads__publisher_platforms') }}

), accounts as (

    select *
    from {{ var('account_history') }}
    where is_most_recent_record = true

), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = true

), ad_sets as (

    select *
    from {{ var('ad_set_history') }}
    where is_most_recent_record = true

), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true

), joined as (

    select
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        creatives.creative_id,
        creatives.creative_name,
        creatives.base_url,
        creatives.url_host,
        creatives.url_path,
        {% raw %}
            CASE WHEN creatives.utm_source LIKE '{{site_source_name}}' THEN publisher_platforms.site_source_name_value
              ELSE creatives.utm_source END AS utm_source,
            CASE WHEN creatives.utm_medium LIKE '{{placement}}' THEN placement_values.placement_value
                ELSE creatives.utm_medium END AS utm_medium,
            CASE WHEN creatives.utm_campaign LIKE '{{adset.name}}' THEN  report.adset_name
                ELSE creatives.utm_campaign END AS utm_campaign,
            CASE WHEN creatives.utm_content LIKE '{{ad.name}}' THEN  report.ad_name
                ELSE creatives.utm_content END AS utm_content,
        {% endraw %}
        creatives.utm_term,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend
    from report
    left join ads
        on cast(report.ad_id as {{ dbt_utils.type_bigint() }}) = cast(ads.ad_id as {{ dbt_utils.type_bigint() }})
    left join creatives
        on cast(ads.creative_id as {{ dbt_utils.type_bigint() }}) = cast(creatives.creative_id as {{ dbt_utils.type_bigint() }})
    left join ad_sets
        on cast(ads.ad_set_id as {{ dbt_utils.type_bigint() }}) = cast(ad_sets.ad_set_id as {{ dbt_utils.type_bigint() }})
    left join campaigns
        on cast(ads.campaign_id as {{ dbt_utils.type_bigint() }}) = cast(campaigns.campaign_id as {{ dbt_utils.type_bigint() }})
    left join accounts
        on cast(report.account_id as {{ dbt_utils.type_bigint() }}) = cast(accounts.account_id as {{ dbt_utils.type_bigint() }})
    left join impression_devices
        on cast(impression_devices.impression_device as {{ dbt_utils.type_string() }}) =  cast(report.impression_device as {{ dbt_utils.type_string() }})
    left join placement_values
        on  cast(placement_values.publisher_platform as {{ dbt_utils.type_string() }}) = cast(report.publisher_platform as {{ dbt_utils.type_string() }})
        and cast(placement_values.platform_position as {{ dbt_utils.type_string() }}) = cast(report.platform_position as {{ dbt_utils.type_string() }})
        and cast(placement_values.impression_device_category as {{ dbt_utils.type_string() }}) =  cast(coalesce(impression_devices.impression_device_category, 'mobile') as {{ dbt_utils.type_string() }})
    left join publisher_platforms
        on cast(publisher_platforms.publisher_platform as {{ dbt_utils.type_string() }}) = cast(report.publisher_platform as {{ dbt_utils.type_string() }})
    {{ dbt_utils.group_by(19) }}

)

select *
from joined