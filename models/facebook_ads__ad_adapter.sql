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

), pre_joined as (

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
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(creatives.utm_source,  '{{site_source_name}}', publisher_platforms.site_source_name_value), '{{placement}}', placement_values.placement_value), '{{campaign.name}}', report.campaign_name), '{{adset.name}}', report.adset_name), '{{ad.name}}', report.ad_name), '{{campaign.id}}', Cast(report.campaign_id AS STRING)), '{{adset.id}}', Cast(report.adset_id AS STRING)), '{{ad.id}}', Cast(report.ad_id AS STRING)) AS utm_source,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(creatives.utm_medium,  '{{site_source_name}}', publisher_platforms.site_source_name_value), '{{placement}}', placement_values.placement_value), '{{campaign.name}}', report.campaign_name), '{{adset.name}}', report.adset_name), '{{ad.name}}', report.ad_name), '{{campaign.id}}', Cast(report.campaign_id AS STRING)), '{{adset.id}}', Cast(report.adset_id AS STRING)), '{{ad.id}}', Cast(report.ad_id AS STRING)) AS utm_medium,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(creatives.utm_campaign,'{{site_source_name}}', publisher_platforms.site_source_name_value), '{{placement}}', placement_values.placement_value), '{{campaign.name}}', report.campaign_name), '{{adset.name}}', report.adset_name), '{{ad.name}}', report.ad_name), '{{campaign.id}}', Cast(report.campaign_id AS STRING)), '{{adset.id}}', Cast(report.adset_id AS STRING)), '{{ad.id}}', Cast(report.ad_id AS STRING)) AS utm_campaign,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(creatives.utm_content, '{{site_source_name}}', publisher_platforms.site_source_name_value), '{{placement}}', placement_values.placement_value), '{{campaign.name}}', report.campaign_name), '{{adset.name}}', report.adset_name), '{{ad.name}}', report.ad_name), '{{campaign.id}}', Cast(report.campaign_id AS STRING)), '{{adset.id}}', Cast(report.adset_id AS STRING)), '{{ad.id}}', Cast(report.ad_id AS STRING)) AS utm_content,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(creatives.utm_term,    '{{site_source_name}}', publisher_platforms.site_source_name_value), '{{placement}}', placement_values.placement_value), '{{campaign.name}}', report.campaign_name), '{{adset.name}}', report.adset_name), '{{ad.name}}', report.ad_name), '{{campaign.id}}', Cast(report.campaign_id AS STRING)), '{{adset.id}}', Cast(report.adset_id AS STRING)), '{{ad.id}}', Cast(report.ad_id AS STRING)) AS utm_term,
        {% endraw %}
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

), joined as (

    select
        pre_joined.date_day,
        pre_joined.account_id,
        pre_joined.account_name,
        pre_joined.campaign_id,
        pre_joined.campaign_name,
        pre_joined.ad_set_id,
        pre_joined.ad_set_name,
        pre_joined.ad_id,
        pre_joined.ad_name,
        pre_joined.creative_id,
        pre_joined.creative_name,
        pre_joined.base_url,
        pre_joined.url_host,
        pre_joined.url_path,
        {% if var('facebook_auto_tagging_enabled', false) %}

        coalesce( pre_joined.utm_source, 'facebook')  as utm_source,
        coalesce( pre_joined.utm_medium , 'cpc') as utm_medium,

        {% else %}

        pre_joined.utm_source,
        pre_joined.utm_medium,

        {% endif %}

        pre_joined.utm_campaign,
        pre_joined.utm_content,
        pre_joined.utm_term,
        pre_joined.clicks,
        pre_joined.impressions,
        pre_joined.spend
    from pre_joined

)

select *
from joined