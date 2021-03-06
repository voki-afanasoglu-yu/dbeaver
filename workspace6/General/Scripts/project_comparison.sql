-- Manor Matters

alter table temp.cohort_MM_all_platforms_2021 add column project varchar(5) default 'mm'

select * from temp.cohort_MM_all_platforms_2021 limit 10

alter table users
add column feedback_score int
default NULL;


create table vokigames.cohort_MM_all_platforms_2021_07_25 as  	
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
--    limit 10
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
		si.install_type,
        si.device_type,
        'gp' as platform
    from
        main_day.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
		si.install_type,
        si.device_type,
        'amz' as platform
    from
        main_day.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second' 
        
        
        
        -- ???????
        
        
SELECT
     (timestamp 'epoch' + day::float * interval '1 second')::date as report_period,
     count(distinct event_user),
     sum(offer_price) as rev,
     sum(offer_price) / count(distinct event_user) as arppu
FROM
    (
    select *
	from vokigames.valid_iap_30975_pq  -- ts ios
	WHERE
    timestamp 'epoch' + day::float * interval '1 second' between '2021-01-01' and '2021-06-30'
	union all 
	select *
	from vokigames.valid_iap_31756_pq  -- ts ios
	WHERE
    timestamp 'epoch' + day::float * interval '1 second' between '2021-01-01' and '2021-06-30'
	union all
	select *
	from vokigames.valid_iap_31761_pq  -- ts ios
	WHERE
    timestamp 'epoch' + day::float * interval '1 second' between '2021-01-01' and '2021-06-30'
    )
        group by 1
        order by 1
        
        
        -- ?????? ? ?????? 
        
        select 
        min(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
        max(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
        count(distinct event_user)
        from
        vokigames.seg_players_30975_pq sp
        where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' = '2021-07-28'
        
        select 
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        min(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
--        max(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
        count(distinct event_user)
        from
        vokigames.seg_players_30975_pq sp
        where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' = '2021-07-28'
        group by 1 
        order by 1
        
        -- ???????? ??????? ? ????? vokigames
        
        create table vokigames.cohort_MM_all_platforms_2021_07_25 as  	
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        vokigames.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
--    limit 10
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
		si.install_type,
        si.device_type,
        'gp' as platform
    from
        vokigames.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
		si.install_type,
        si.device_type,
        'amz' as platform
    from
        vokigames.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-25'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-25'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second' 
        
        
        select 
        	first_session_date,
        	count(event_user) as cohort_size
        from cohort_MM_all_platforms_2021
        group by 1
        order by 1
        
        -- ??? ?????? 
        
        
        drop table vokigames.MM_all_platforms_all_players
        
        create table vokigames.MM_all_platforms_all_players as  	
		select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        vokigames.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'gp' as platform
    from
        vokigames.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
                (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'amz' as platform
    from
        vokigames.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
with aver as
        (
        select 
        	activity_date::text,
        	count(distinct event_user) as au,
        	count(distinct 
        	case 
        	when (payer = 1 and rev>0) then	event_user
        	end
        	) as pu,
        	sum(rev) as revenue,
        	sum(rev) * 1.0 / count(distinct event_user) as arpu_total,
        	sum(rev) * 1.0 / count(distinct 
        	case 
        	when (payer = 1 and rev>0) then	event_user
        	end
        	) as arppu_total
        from vokigames.MM_all_platforms_all_players
        where activity_date between '2021-06-01' and '2021-06-30'
        group by 1
        )
        select * from aver
        union all
        select
        'total' as activity_date,
        avg(au) as au,
        avg(pu) as pu,
        avg(revenue) as revenue,
        avg(arpu_total) as arpu_total,
        avg(arppu_total) as arppu_total
        from aver
        group by 1
		order by 1
        	
        	
        select *	
        from vokigames.MM_all_platforms_all_players limit 10
        
        
        
        -- ??? ?????? HS
        
        select * from impala_main_day.seg_players_4698_pq limit 10
        
        
        create table vokigames.HS_all_platforms_all_players as  	
		select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        impala_main_day.seg_players_4698_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_4698_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'gp' as platform
    from
        impala_main_day.seg_players_4699_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_4699_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
                (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'amz' as platform
    from
        impala_main_day.seg_players_4700_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_4700_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
--        GS
        
        -- ??? ?????? GS
        
        
        create table vokigames.GS_all_platforms_all_players as  	
		select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        impala_main_day.seg_players_3789_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3789_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_3789_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'gp' as platform
    from
        impala_main_day.seg_players_3790_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3790_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_3790_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
                (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'amz' as platform
    from
        impala_main_day.seg_players_3791_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3791_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_3791_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
        
        select *
        from vokigames.MM_all_platforms_all_players limit 10
        
        with aver as
        (
        select 
        	activity_date::text,
        	count(distinct event_user) as au,
        	count(distinct 
        	case 
        	when payer = 1 then	event_user
        	end
        	) as pu,
        	sum(rev) as revenue,
        	sum(rev) * 1.0 / count(distinct event_user) as arpu_total,
        	sum(rev) * 1.0 / count(distinct 
        	case 
        	when payer = 1 then	event_user
        	end
        	) as arppu_total
        from vokigames.MM_all_platforms_all_players
        where activity_date between '2021-04-01' and '2021-06-30'
        group by 1
        )
        select * from aver
        union all
        select
        'total' as activity_date,
        avg(au) as au,
        avg(pu) as pu,
        avg(revenue) as revenue,
        avg(arpu_total) as arpu_total,
        avg(arppu_total) as arppu_total
        from aver
        group by 1
		order by 1
        
        
        
        select *
        from vokigames.cohort_MM_all_platforms_30_days_202106
        limit 10
        
        select 
        	activity_date,
        	count(distinct event_user) as au,
        	count(distinct 
        	case 
        	when payer = 1 then	event_user
        	end
        	) as pu,
        	sum(rev) as revenue,
        	sum(rev) * 1.0 / count(distinct event_user) as arpu_total,
        	sum(rev) * 1.0 / count(distinct 
        	case 
        	when payer = 1 then	event_user
        	end
        	) as arppu_total
        from vokigames.cohort_MM_all_platforms_30_days_202106
        group by 1
        order by 1
        
        
        select 
        	first_session_date,
        	count(event_user) as cohort_size
        from cohort_MM_all_platforms_2021
        group by 1
        order by 1
        
        
        select 
        min(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
        max(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second' ),
        count(distinct event_user)
        from
        vokigames.seg_players_30975_pq sp
        where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        
        
        
        
        select * from temp.cohort_databricks_hs_ios_2021 limit 10
        
        
        
 -- HOMESCAPES
        
        --IOS
        
 alter table temp.cohort_hs_all_platforms_2021 add column project varchar(5) default 'hs'
 
  select * from temp.cohort_hs_all_platforms_2021 limit 10
        
 drop table temp.cohort_hs_ios_2021 
        
 create table temp.cohort_hs_ios_6_2021 as  	
 select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'ios' as platform,
        'hs' as project
    from
        main_day.seg_players_4698_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4698_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
 
        )
         select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from events 
        group by 1,2
        order by 1, 3 desc
        
        
--    limit 10
--union all
----GP
        create table temp.cohort_hs_gp_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'gp' as platform
    from
        main_day.seg_players_4699_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4699_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
--union all
---- AMAZON
        create table temp.cohort_hs_amz_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'amz' as platform
    from
        main_day.seg_players_4700_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4700_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second' 
--  limit 30      
        
        
  -- GARDENSCAPES
        
        --IOS
        
  alter table temp.cohort_gs_all_platforms_2021 add column project varchar(5) default 'gs'
  
      select * from temp.cohort_gs_all_platforms_2021 limit 10
        
 drop table temp.cohort_gs_ios_2021 
        
 create table temp.cohort_gs_ios_2021 as  	
 select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'ios' as platform
    from
        main_day.seg_players_3789_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_3789_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_3789_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
 
        )
         select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from events 
        group by 1,2
        order by 1, 3 desc
        
        
--    limit 10
--union all
----GP
        create table temp.cohort_gs_gp_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'gp' as platform
    from
        main_day.seg_players_3790_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_3790_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4699_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
--union all
---- AMAZON
        create table temp.cohort_gs_amz_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'amz' as platform
    from
        main_day.seg_players_3791_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_3791_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_3791_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second' 
        
    
        
 -- Wildscapes
 
        -- IOS
        
    alter table temp.cohort_ws_all_platforms_2021 add column project varchar(5) default 'ws'
    
    select * from temp.cohort_ws_all_platforms_2021 limit 10
        
     create table temp.cohort_ws_ios_2021 as  	
 select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'ios' as platform
    from
        main_day.seg_players_3928_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_3928_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_3928_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
 
      
        
--    limit 10
--union all
----GP
        create table temp.cohort_ws_gp_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'gp' as platform
    from
        main_day.seg_players_4761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_4761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
--union all
---- AMAZON
        create table temp.cohort_ws_amz_2021 as
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'amz' as platform
    from
        main_day.seg_players_31119_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_31119_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31119_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'     
        
        
        
        -- METRICS
        
        select 
        	count (distinct event_user) as unique_users,
        	sum (rev) as revenue,
        	sum (orders) as orders,
        	sum (rev) * 1.0  / sum (orders) as avg_order
        from cohort_mm_all_platforms_2021
        
        
        
        select 
        	activity_month ,
        	platform,
        	count(distinct event_user) as unique_users,
        	sum (rev) as revenue,
        	sum (orders) as orders,
        	sum (rev) * 1.0  / sum (orders) as avg_order
        from cohort_mm_all_platforms_2021
        group by 1,2
        union all 
        select 
        	first_session_month,
        	'total' as plarform,
        	count(distinct event_user) as unique_users,
        	sum (rev) as revenue,
        	sum (orders) as orders,
        	sum (rev) * 1.0  / sum (orders) as avg_order
        from cohort_mm_all_platforms_2021
		group by 1
        order by 1, 3 desc
        
        -- MANOR MATTERS
        -- week cohorts
        
        -- total month revenue
        

        select 
        	activity_month,
        	'total' as country,
        	sum(rev)
        from cohort_mm_all_platforms_2021
        group by 1,2
        union all 
        select 
        	activity_month,
        	device_region as country,
        	sum(
        	case 
        	when rev is null then 0
        	else rev 
        	end
        	) as rev
        from cohort_mm_all_platforms_2021
        group by 1,2
        order by 1, 3 desc
        
        
        select * from cohort_mm_all_platforms_2021 limit 10
        
        create table temp.week_cohorts_MM_06_2021 as
        with cohort_size_period as 
        (
        select 
        	first_session_week,
        	activity_week,
        	count(distinct event_user) as cohort_size_period,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from cohort_mm_all_platforms_2021
--        where payer = 1
        group by 1,2
        )
        ,
        paying_cohort as
        (
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size.cohort_size_paying,
        	cohort_size_period *1.0 / cohort_size as rr,
        	cohort_size_period_paying *1.0 / cohort_size_paying as rr_paying,
        	revenue / cohort_size as arpu,
        	revenue / cohort_size_paying as arppu,
        	revenue / cohort_size_period_paying as arpppu,
        	orders * 1.0 / cohort_size as full_order_freq,
        	orders * 1.0 / cohort_size_paying as paying_order_freq,
        	cohort_size_paying *1.0 / cohort_size as paying_cohort_share
        from cohort_size_period
        left join (
		select 
        	first_session_week,
        	count(distinct event_user) as cohort_size,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_paying
        from cohort_mm_all_platforms_2021
--        where payer = 1
        group by 1
        order by 1
        ) as cohort_size using(first_session_week)
		order by 1,2
       )
       select 
       *       
       from paying_cohort
        
        
        -- day cohorts MM
        
       create table temp.day_cohorts_MM_06_2021 as
        with cohort_size_period as 
        (
        select 
        	first_session_date,
        	activity_date,
        	count(distinct event_user) as cohort_size_period,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from cohort_mm_all_platforms_2021
--        where payer = 1
        group by 1,2
        )
        ,
        paying_cohort as
        (
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size.cohort_size_paying,
        	cohort_size_period *1.0 / cohort_size as rr,
        	cohort_size_period_paying *1.0 / cohort_size_paying as rr_paying,
        	revenue / cohort_size as arpu,
        	revenue / cohort_size_paying as arppu,
        	revenue / cohort_size_period_paying as arpppu,
        	orders * 1.0 / cohort_size as full_order_freq,
        	orders * 1.0 / cohort_size_paying as paying_order_freq,
        	cohort_size_paying *1.0 / cohort_size as paying_cohort_share
        from cohort_size_period
        left join (
		select 
        	first_session_date,
        	count(distinct event_user) as cohort_size,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_paying
        from temp.cohort_mm_all_platforms_2021
--        where payer = 1
        group by 1
        order by 1
        ) as cohort_size using(first_session_date)
		order by 1,2
       )
       select 
       *       
       from paying_cohort 
       
       
       
        -- day cohorts HS
        create table temp.day_cohorts_HS_06_2021 as
        with full_data_hs as
        (
        select *
        from temp.cohort_hs_gp_2021
        union all
        select *
        from temp.cohort_hs_ios_2021
        )
        ,
        cohort_size_period as 
        (
        select 
        	first_session_date,
        	activity_date,
        	count(distinct event_user) as cohort_size_period,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from full_data_hs
--        where payer = 1
        group by 1,2
        )
        ,
        paying_cohort as
        (
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size.cohort_size_paying,
        	cohort_size_period *1.0 / cohort_size as rr,
        	cohort_size_period_paying *1.0 / cohort_size_paying as rr_paying,
        	revenue / cohort_size as arpu,
        	revenue / cohort_size_paying as arppu,
        	revenue / cohort_size_period_paying as arpppu,
        	orders * 1.0 / cohort_size as full_order_freq,
        	orders * 1.0 / cohort_size_paying as paying_order_freq,
        	cohort_size_paying *1.0 / cohort_size as paying_cohort_share
        from cohort_size_period
        left join (
		select 
        	first_session_date,
        	count(distinct event_user) as cohort_size,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_paying
        from full_data_hs
--        where payer = 1
        group by 1
        order by 1
        ) as cohort_size using(first_session_date)
		order by 1,2
       )
       select 
       *       
       from paying_cohort  
       
       
       select * from cohort_mm_all_platforms_2021 where lt_period_month < 0 
       
       select * from day_cohorts_MM_06_2021
       order by 1,2
       
       
       -- ?????? ??????? ?? ????????? HS
       
       drop table  temp.cohort_hs_all_platforms_2021
       
       create table  temp.cohort_hs_all_platforms_2021 as
        select *
        from temp.cohort_hs_gp_2021
        union all
        select *
        from temp.cohort_hs_ios_6_2021
        union all
        select *
        from temp.cohort_hs_amz_2021
        
        select * from temp.cohort_hs_all_platforms_2021 limit 10
        
        -- day cohorts HS
        
        drop table temp.day_cohorts_HS_06_2021
        
        create table temp.day_cohorts_HS_06_2021 as
        with cohort_size_period as 
        (
        select 
        	first_session_date,
        	activity_date,
        	count(distinct event_user) as cohort_size_period,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from temp.cohort_hs_all_platforms_2021
--        where payer = 1
        group by 1,2
        )
        ,
        paying_cohort as
        (
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size.cohort_size_paying,
        	cohort_size_period *1.0 / cohort_size as rr,
        	cohort_size_period_paying *1.0 / cohort_size_paying as rr_paying,
        	revenue / cohort_size as arpu,
        	revenue / cohort_size_paying as arppu,
        	revenue / cohort_size_period_paying as arpppu,
        	orders * 1.0 / cohort_size as full_order_freq,
        	orders * 1.0 / cohort_size_paying as paying_order_freq,
        	cohort_size_paying *1.0 / cohort_size as paying_cohort_share
        from cohort_size_period
        left join (
		select 
        	first_session_date,
        	count(distinct event_user) as cohort_size,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_paying
        from temp.cohort_hs_all_platforms_2021
--        where payer = 1
        group by 1
        order by 1
        ) as cohort_size using(first_session_date)
		order by 1,2
       )
       select 
       *       
       from paying_cohort
       
       
--       ??????? ?? ??????? 
       
       select * from cohort_hs_ios_2021 limit 10
       
       select 
       activity_month,
       sum(rev)
       from cohort_hs_ios_2021 
       group by 1
       order by 1
       
       
       
       select 
       activity_month,
       sum(rev)
       from cohort_hs_ios_6_2021
       group by 1
       order by 1
       
       select 
       activity_month,
       sum(rev)
       from temp.cohort_hs_gp_2021
       group by 1
       order by 1       
       
       
       
       -- ?????? ??????? ?? ????????? GS
       
       create table  temp.cohort_gs_all_platforms_2021 as
        select *
        from temp.cohort_gs_gp_2021
        union all
        select *
        from temp.cohort_gs_ios_2021
        union all
        select *
        from temp.cohort_gs_amz_2021
        
        
        -- ?????? ??????? ?? ????????? WS
        
        
       
       create table  temp.cohort_ws_all_platforms_2021 as
        select *
        from temp.cohort_ws_gp_2021
        union all
        select *
        from temp.cohort_ws_ios_2021
        union all
        select *
        from temp.cohort_ws_amz_2021
        
         select * from temp.cohort_ws_all_platforms_2021 limit 10
         
         
         ALTER TABLE "temp".cohort_mm_all_platforms_2021 owner to afanasoglu_yu;
        
        grant select on "temp".cohort_mm_all_platforms_2021 to kirichko_a;
        grant select on "temp".cohort_hs_all_platforms_2021 to kirichko_a;
        grant select on "temp".cohort_gs_all_platforms_2021 to kirichko_a;
        grant select on "temp".cohort_ws_all_platforms_2021 to kirichko_a;
       
       grant select on vokigames.MM_all_platforms_all_players to kirichko_a;
       
       
       with sources as
       (
       select * from "temp".cohort_ws_all_platforms_2021 
       union all
       select * from "temp".cohort_mm_all_platforms_2021 
       union all
       select * from "temp".cohort_hs_all_platforms_2021 
       union all
       select * from "temp".cohort_gs_all_platforms_2021 
       )
       
       
       create table vokigames.project_payers_06_2021 as
       select 
       project,
       count ( distinct event_user) as installs,
       sum(rev) as revenue,
	   sum(orders) as orders,
       count ( distinct
       case
       when payer = 1 then event_user
       end 
       ) as payers,
       count ( distinct
       case
       when payer = 0 then event_user
       end 
       ) as nonpayers,
       count ( distinct
       case
       when payer = 1 then event_user
       end 
       ) * 1.0 / count (distinct event_user) as payer_share
       from vokigames.all_project_compare_06_2021
       group by 1
       
       
     select * from "temp".cohort_ws_all_platforms_2021 limit 10  
     
       
     create table vokigames.all_project_compare_06_2021 as
     select * from "temp".cohort_ws_all_platforms_2021 
       union all
       select * from "temp".cohort_mm_all_platforms_2021 
       union all
       select * from "temp".cohort_hs_all_platforms_2021 
       union all
       select * from "temp".cohort_gs_all_platforms_2021 
       
       select count(*) from  vokigames.all_project_compare_06_2021
       
       select * from  vokigames.all_project_compare_06_2021 limit 10
       
       -- ??????????? ?? ???????
       
       create table vokigames.projects_countries_payers_06_2021 as
        select 
	       project,
	       device_region as country,
	       sum(rev) as revenue,
	       sum(orders) as orders,
	       count ( distinct
	       case
	       when payer = 1 then event_user
	       end 
	       ) as payers,
	       count ( distinct
	       case
	       when payer = 0 then event_user
	       end 
	       ) as nonpayers,
	       count ( distinct
	       case
	       when payer = 1 then event_user
	       end 
	       ) * 1.0 / count (distinct event_user) as payers_share
	       from vokigames.all_project_compare_06_2021
	       group by 1,2 
	       order by 1,3 desc
	       
	       
	       select 
	       *
	       from projects_countries_payers_06_2021
	       
	       with country_rank as 
	       (
	       select 
	       *,
	       rank() over (partition by project order by revenue desc) as rev_rank
	       from projects_countries_payers_06_2021
	       where revenue notnull
       )
       select * from country_rank
       
        -- ??????????? ?? ??????????
       
       create table vokigames.projects_platform_payers_06_2021 as
	   select 
       project,
       platform,
       sum(rev) as revenue,
       sum(orders) as orders,
       count ( distinct
       case
       when payer = 1 then event_user
       end 
       ) as payers,
       count ( distinct
       case
       when payer = 0 then event_user
       end 
       ) as nonpayers,
       count ( distinct
       case
       when payer = 1 then event_user
       end 
       ) * 1.0 / count (distinct event_user) as payers_share
       from vokigames.all_project_compare_06_2021
       group by 1,2 
       order by 1,3 desc
       
       
       select * from vokigames.device_region limit 10
       
       select * from vokigames.all_events_30975_pq limit 10;
      
      select * from vokigames.seg_players_4700_pq limit 10;
     
       select * from vokigames.seg_install_info_30975_pq limit 10;
      
      select * from vokigames.seg_install_info_31756_pq limit 10;
       
       select 
       	two.install_type,
       	one.*
      from 
      (
      select * from "temp".cohort_mm_all_platforms_2021 where platform = 'ios' limit 10
      ) as one
      left join 
      (
      select * 
      from 
      vokigames.seg_install_info_30975_pq si
      where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
      ) as two on one.event_user = two.event_user
      
      
       drop table vokigames.test_alter
      
      create table vokigames.test_alter as
      select * from "temp".cohort_mm_all_platforms_2021 where platform = 'ios' limit 10
      
      
      select * from vokigames.test_alter 
      
  	alter table vokigames.test_alter add column install_type varchar(10) default ''
  	
  	create 
 	SELECT install_type
	from 
      vokigames.seg_install_info_30975_pq si
      where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30' 
--      and event_user in (select distinct event_user from vokigames.test_alter)
      
      select * from vokigames.cohort_MM_all_platforms_2021_07_25 limit 10
  	
      
      
    --  hs arpu
      
      
      with sources as
      
      
      select 
      first_session_date,
      count(distinct event_user)  as cohort_size
      from vokigames.all_project_compare_06_2021
      where project = 'hs' and first_session_date >= '2021-05-01' and first_session_date = activity_date 
      group by 1
      order by 1
      limit 10
      
      select *,
      count(distinct event_user) over (partition by first_session_date) as cohort_size
      from vokigames.all_project_compare_06_2021
      where project = 'hs' and first_session_date >= '2021-05-01'
      limit 10
      
      
      from 
      
      
      
      with rev as
      (
      select 
      	first_session_date,
      	lt_period_day,
      	sum(rev) as revenue
      from vokigames.all_project_compare_06_2021 
      where project = 'hs' and first_session_date >= '2021-06-01'
      group by 1,2
      order by 1,2
      )
      ,
      cohort_size as 
      (
      select 
      first_session_date,
      count(distinct event_user)  as cohort_size
      from vokigames.all_project_compare_06_2021
      where project = 'hs' and first_session_date >= '2021-06-01' and first_session_date = activity_date 
      group by 1
      order by 1      
      )
      select 
      *, 
      sum(revenue * 1.0 / cohort_size) over (partition by rev.first_session_date order by lt_period_day rows unbounded preceding) as arppu_cum,
      revenue * 1.0 / cohort_size as arpu
      from rev left join cohort_size on rev.first_session_date = cohort_size.first_session_date
      order by 1,2
      
      
      with rev as
      (
      select 
      	first_session_date,
      	lt_period_day,
      	sum(rev) as revenue
      from vokigames.all_project_compare_06_2021 
      where project = 'ws' and first_session_date >= '2021-04-01'
      group by 1,2
      order by 1,2
      )
      ,
      cohort_size as 
      (
      select 
      first_session_date,
      count(distinct event_user)  as cohort_size
      from vokigames.all_project_compare_06_2021
      where project = 'mm' and first_session_date >= '2021-04-01' and first_session_date = activity_date 
      group by 1
      order by 1      
      )
      ,
      cohort_size_period as 
      (
      select 
      first_session_date,
      lt_period_day,
      count(distinct event_user)  as cohort_size_period
      from vokigames.all_project_compare_06_2021
      where project = 'mm' and first_session_date >= '2021-04-01' --and first_session_date = activity_date 
      group by 1,2
      order by 1,2      
      )
      select 
      *, 
      sum(revenue * 1.0 / cohort_size) over (partition by rev.first_session_date order by rev.lt_period_day rows unbounded preceding) as arppu_cum,
      revenue * 1.0 / cohort_size as arpu,
      cohort_size_period * 1.0 / cohort_size as rr
      from rev 
      left join cohort_size on rev.first_session_date = cohort_size.first_session_date
      left join cohort_size_period on (rev.first_session_date = cohort_size_period.first_session_date and rev.lt_period_day = cohort_size_period.lt_period_day)
      order by 1,2
      
 with arpu_total     as
(      select 
      	activity_date,
      	project,
      	count(distinct event_user) as AU,
      	sum(rev) as revenue,
      	sum(rev) *1.0 / count(distinct event_user) as arpu_total
      from vokigames.all_project_compare_06_2021
      where activity_date >= '2021-06-01'
      group by 1,2
      order by 1,2)
      select 
      project,
      avg(au) as au,
      avg(revenue) as revenue,
      avg(arpu_total) as arpu_total
      from arpu_total
      group by 1
      order by 1
      
      
 with arppu_total     as
(      select 
      	activity_date,
      	project,
      	count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as pu,
      	sum(rev) as revenue,
      	sum(rev) *0.7 / count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as arppu_total
      from vokigames.all_project_compare_06_2021
      where activity_date >= '2021-06-01'
      group by 1,2
      order by 1,2)
      select 
      project,
      avg(pu) as pu,
      avg(revenue) as revenue,
      avg(arppu_total) as arppu_total
      from arppu_total
      group by 1
      order by 1 
      
      
       with arppu_total     as
(      select 
      	activity_date,
      	project,
      	count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as pu,
      	sum(rev) as revenue,
      	sum(rev) *0.7 / count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as arppu_total
      from vokigames.all_project_compare_06_2021
      where activity_date >= '2021-04-01'
      group by 1,2
      order by 1,2)
      select 
      project,
      avg(pu) as pu,
      avg(revenue) as revenue,
      avg(arppu_total) as arppu_total
      from arppu_total
      group by 1
      order by 1
      
      
       select 
      	activity_date
      	project,
      	count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as pu,
      	sum(rev) as revenue,
      	sum(rev) *1.0 / count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as arppu_total
      from vokigames.cohort_mm_all_platforms_2021_07_25
      where activity_date >= '2021-06-01'
      group by 1,2
      order by 1,2
      
      
      select * from vokigames.cohort_mm_all_platforms_2021_07_25 limit 10
      
      select 
      	activity_date,
      	count(distinct 
      	case
      	when (payer = 1 and rev > 0) then event_user
      	end
      	) as pu,
      	sum(rev) as revenue,
      	sum(rev) * 0.7 / count(distinct 
      	case
      	when (payer = 1 and rev>0) then event_user
      	end
      	) as arppu_total
      from vokigames.mm_all_platforms_all_players
      where activity_date >= '2021-06-01'
      group by 1
      order by 1
      	
      
      
      
       with arppu_total     as
(      select 
      	activity_date,
      	project,
      	count(distinct 
      	case
      	when payer = 1 then event_user
      	end
      	) as pu,
      	sum(rev) as revenue,
      	sum(rev) *1.0 / count(distinct 
      	case
      	when payer = 1 then event_user
      	end
      	) as arppu_total
      from vokigames.MM_all_platforms_all_players
      where activity_date >= '2020-04-01'
      group by 1,2
      order by 1,2)
      select 
      project,
      avg(pu) as au,
      avg(revenue) as revenue,
      avg(arppu_total) as arppu_total
      from arppu_total
      group by 1
      order by 1 	
      
      
      select * from vokigames.mm_all_platforms_all_players limit 10
      
      select 
      	activity_date,
      	platform,
      	count (distinct event_user) as DAU
      from vokigames.mm_all_platforms_all_players
      where activity_date >= '2021-07-20' and platform = 'ios'
      group by 1,2
      order by 1
      
      
            select 
      	* 
      	from vokigames.seg_players_30975_pq
      	limit 10
      
      select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	count (distinct sp.event_user) as DAU
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-06-01' and '2021-07-31'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-06-01' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-06-01' and '2021-07-31'
        ) si on sp.event_user = si.event_user
        group by 1
      	order by 1
      	
      	
      	
      	
      	-- ????? ???? 2021-08-04 MM
      	
      	-- IOS
      	-- ?????????? ??????!!!
      	
      	drop table vokigames.ios_test_2021
      	
--      	vokigames.mm_2020_05_2021_07
      	
      	-- MM
      	      	create table vokigames.mm_2020_05_2021_08 as
      	      	WITH tmp_variables AS (
		SELECT 
		   '2020-04-30'::DATE AS StartDate, 
		   '2021-08-04'::DATE AS FinishDate
		)
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_date,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' =  (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_date,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'gp' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_31756_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between   (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second' between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_31756_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session / 1000 = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_date,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'amz' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_31761_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_31761_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session / 1000 = si.first_session
        
        
        	      	WITH tmp_variables AS (
		SELECT 
		   '2021-07-20'::DATE AS StartDate, 
		   '2021-08-04'::DATE AS FinishDate
		),
		all_data as 
		(
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_date,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between  (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' =  (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session
        )
        select * from all_data limit 10
        
        	activity_date,
        	count(distinct event_user) dau,
        	sum(rev)*.7 as rev 
        	from all_data
        group by 1 
        order by 1
        
        select 
        	first_session_date::date,
        	activity_date::date - first_session_date::date,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev) rev
        from vokigames.mm_2020_05_2021_07 
        where  first_session_date >='2021-07-20' --and --first_session_date::date = activity_date::date
        group by 1,2
        order by 1,2
        
        select * from vokigames.hs_2020_05_2021_07 limit 10
        
                select 
        	first_session_date::date,
        	activity_date::date - first_session_date::date,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev) rev
        from vokigames.hs_2020_05_2021_07 
        where  first_session_date >= '2021-07-20' --and '2021-07-05' --and --first_session_date::date = activity_date::date
        group by 1,2
        order by 1,2
        
        
                        select 
        	first_session_date::date,
        	activity_date::date - first_session_date::date,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev) rev
        from vokigames.hs_2020_05_2021_07 
        where  first_session_date >= '2021-07-20' and platform = 'ios' --and '2021-07-05' --and --first_session_date::date = activity_date::date
        group by 1,2
        order by 1,2
        
                select 
        	first_session_date::date,
--        	activity_date::date - first_session_date::date,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev) rev
        from vokigames.mm_2020_05_2021_07 
        where  first_session_date >='2021-07-20' --and --first_session_date::date = activity_date::date
        group by 1
        order by 1
        
        
        -- GS
      	      	create table vokigames.gs_2020_05_2021_07 as
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_3789_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3789_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_3789_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'gp' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_3790_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3790_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_3790_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'amz' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_3791_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3791_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_3791_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        
        
        
         -- HS
      	      	create table vokigames.hs_2020_05_2021_07 as
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4698_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4698_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'gp' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4699_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4699_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        union all
              	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'amz' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4700_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4700_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        
        
        
        select 
        	timestamp 'epoch' + day::float * interval '1 second' as day,
        	sum(offer_price)
        from vokigames.valid_iap_30975_pq
        where 
		timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-25' and '2021-08-03'
        group by 1
        order by 1
        
        
        
        
        select 
        activity_day,
        count(distinct event_user) as DAU,
        sum(rev)*.7 as revenue
        from vokigames.mm_2020_05_2021_07
        where activity_day >= '2021-07-20' and platform = 'ios'
        group by 1
        order by 1
      	
      	create table vokigames.mm_2020_05_2021_07 as
      	with sources as
      	(
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.hour,
      	rev.day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-20' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-20' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-07-20' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        )
                select -- hour, day
            activity_day,
        	count (distinct event_user) dau, 
        	sum(rev)*.7 rev
        	from sources 
        	group by 1 
        	order by 1
        	
        	
        	                select
--                  event_user,
                  day, --timestamp 'epoch' + day::float * interval '1 second' 
                  sum(offer_price) * .7 as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second' between '2021-07-20' and '2021-08-03'
                  group by 1
                  order by 1
        	
        	
        	select 
        	activity_date,
        	count (distinct event_user) dau, 
        	sum(rev)*.7 rev
        	from vokigames.cohort_mm_all_platforms_2021_07_25
        	where activity_date between '2021-07-20' and '2021-08-03'  and platform = 'ios'
        	group by 1 
        	order by 1
        
        
        
        
        select 
        	activity_day,
        	count (distinct event_user) dau, 
        	sum(rev) rev
        	from vokigames.ios_test_2021
        	where activity_day >= '2021-07-01'
        	group by 1 
        	order by 1
        
        
        -- GOOGLE PLAY
        
        create table vokigames.mm_gp_2021_01_08_03 as
        
        with sources as 
        (
      	select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.event_user,
         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'gp' as platform
      	from 
      	(
      	select 
      	*
      	from vokigames.seg_players_31756_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-20' and '2021-08-03'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
      	left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-20' and '2021-08-03'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_31756_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-07-20' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        )
        select 
        	activity_day,
        	count (distinct event_user) as dau,
        	sum(rev)*.7
        	from sources
        	group by 1 
        	order by 1
        	
        	
                with sources as 
                (
        	select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
--        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
--        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
--        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
--        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
--        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        vokigames.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-20' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_30975_pq si on (sp.event_user = si.event_user )
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-20' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        )
        	        select 
        	activity_date,
        	count (distinct event_user) as dau,
        	sum(rev) *0.7
        	from sources
        	group by 1 
        	order by 1
        	
        	
        	
        	select * from vokigames.seg_install_info_30975_pq si limit 10
        
        select 
        distinct event_user,
        min(si.hour) over (partition by event_user) as install_time,
        first_value (install_type) over (partition by event_user)
--        first_value (device_type) over (partition by event_user order by si."hour")
        from	vokigames.seg_install_info_30975_pq si
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-07-20' and '2021-08-03'  --and event_user = '30112019-124455-ZbMbeJdF' and hour = '1627776000'
        limit 10
--        group by 1,2
--        order by 3 desc
        	
        	
--        limit 10
      	
      	
      	
        
              select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
      	sp.event_user,
        timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
      	'ios' as platform,
      	count (distinct sp.event_user) as DAU
      	from vokigames.seg_players_30975_pq sp
      	left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-01-01' and '2021-07-31'
        ) si on sp.event_user = si.event_user
        where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-07-31'
--      	and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2021-07-25' and '2021-07-31'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
        
        
        
        
      	
      	
      	
      	union all
      	      select 
      	timestamp 'epoch' + (hour - 86400)::float * interval '1 second' as activity,
      	'gp' as platform,
      	count (distinct event_user) as DAU
      	from vokigames.seg_players_31756_pq
      	where timestamp 'epoch' + (hour - 86400)::float * interval '1 second'  between '2021-07-20' and '2021-07-31'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (hour)::float * interval '1 second'
      	group by 1
      	union all 
      	      select 
      	timestamp 'epoch' + (hour - 86400)::float * interval '1 second' as activity,
      	'amz' as platform,
      	count (distinct event_user) as DAU
      	from vokigames.seg_players_31761_pq
      	where timestamp 'epoch' + (hour - 86400)::float * interval '1 second'  between '2021-07-20' and '2021-07-31'
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (hour)::float * interval '1 second'
      	group by 1
      	order by 1
      	
      	
--      	limit 10
      	
      	
      	
      	with sources as
      	(
      	select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second') as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'ios' as platform
    from
        vokigames.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-31'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second') as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'gp' as platform
    from
        vokigames.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
	left join vokigames.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  ='2021-07-31'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
	union all
            select
                (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second') as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        si.install_type,
        si.device_type,
        'amz' as platform
    from
        vokigames.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
	left join vokigames.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-07-25' and '2021-07-31'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-31'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
      	)
      	select 
      		activity_date,
      		platform,
      		count (distinct event_user) as DAU
      	from sources
      	group by 1,2
      	order by 1
      	
      	 grant select on vokigames.cohort_mm_all_platforms_2021_07_25 to kirichko_a;
      	
      	grant select on vokigames.mm_cohorts_2021 to kirichko_a;
      
      
      