{{ config(
    materialized="ephemeral"
)}}

with placement_values as (
    select 'facebook' as publisher_platform, 'feed' as platform_position, 'mobile' as impression_device_category, 'Facebook_Mobile_Feed' as placement_value

    union all 
    
    select 'facebook' as publisher_platform, 'feed' as platform_position, 'desktop' as impression_device_category, 'Facebook_Desktop_Feed' as placement_value
    
    union all 
    
    select 'facebook' as publisher_platform, 'right_hand_column' as platform_position, null as impression_device_category, 'Facebook_Right_Column' as placement_value
    
    union all 
    
    select 'facebook' as publisher_platform, 'marketplace' as platform_position, null as impression_device_category, 'Facebook_Marketplace' as placement_value
    
    
    union all 
    
    select 'facebook' as publisher_platform, 'facebook_stories' as platform_position, null as impression_device_category, 'Others' as placement_value
    
    union all 
    
    select 'facebook' as publisher_platform, 'instant_article' as platform_position, null as impression_device_category, 'Facebook_Instant_Articles' as placement_value
    
    union all 
    
    select 'facebook' as publisher_platform, 'video_feeds' as platform_position, 'mobile' as impression_device_category, 'Facebook_Mobile_Feed' as placement_value
    
    union all 
    
    select 'instagram' as publisher_platform, 'instagram_stories' as platform_position, null as impression_device_category, 'Instagram_Stories' as placement_value
    
    union all 
    
    select 'instagram' as publisher_platform, 'feed' as platform_position, null as impression_device_category, 'Instagram_Feed' as placement_value
    
    union all 
    
    select 'instagram' as publisher_platform, 'instagram_explore' as platform_position, null as impression_device_category, 'Instagram_Explore' as placement_value
    
    union all 
    
    select 'audience_network' as publisher_platform, 'an_classic' as platform_position, null as impression_device_category, 'an' as placement_value
    
    union all 
    
    select 'audience_network' as publisher_platform, 'instream_video' as platform_position, null as impression_device_category, 'Facebook_Instream_Video' as placement_value
    
    union all 
    
    select 'messenger' as publisher_platform, 'messenger_inbox' as platform_position, null as impression_device_category, 'Messenger_Inbox' as placement_value
    
    union all 
    
    select 'messenger' as publisher_platform, 'messenger_stories' as platform_position, null as impression_device_category, 'Others' as placement_value
    
    union all 
    
    select 'facebook' as publisher_platform, 'instream_video' as platform_position, null as impression_device_category, 'Facebook_Instream_Video' as placement_value /*not sure of the value*/
    
    union all 
    
    select 'facebook' as publisher_platform, 'search' as platform_position, null as impression_device_category, 'Others' as placement_value /*not sure of the value*/
    
    union all 
    
    select 'audience_network' as publisher_platform, 'rewarded_video' as platform_position, null as impression_device_category, 'Facebook_Rewarded_Video' as placement_value /*not sure of the value*/
)

select * from placement_values