drop table temp.cohort_gs_gp_2021

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
left join main_day.seg_install_info_3790_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
        with gs as 
        (
        select * from temp.cohort_gs_ios_2021
        union all
        select * from temp.cohort_gs_gp_2021
        union all
        select * from temp.cohort_gs_amz_2021
        )
        select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from  gs
        group by 1,2
        union all 
		select 
			activity_month,
			'total' platform,
			sum(rev) as revenue
		from gs
        group by 1,2
		order by 1, 3 desc
        
        
        select count(*) from  temp.cohort_gs_gp_2021
        
        
        -- all players hs
        
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
        vokigames.seg_players_4698_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_4698_pq si on sp.event_user = si.event_user
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
        vokigames.seg_players_4699_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_4699_pq si on sp.event_user = si.event_user
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
        vokigames.seg_players_4700_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from vokigames.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join vokigames.seg_install_info_4700_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-28'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-28'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
--       все игроки
  
--        HS
        
        

        
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
        
        --        GP
        
        
               create table vokigames.HS_GP_all_players as  	
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
                  timestamp 'epoch' + day::float * interval '1 second'  between '2019-09-26' and '2021-07-31'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join impala_main_day.seg_install_info_4699_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2019-09-26' and '2021-07-31'
--        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-07-31'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        --        ios
        
        
               create table vokigames.HS_ios_all_players as  	
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
        
        
        
        -- амазон
        
        create table vokigames.HS_amz_all_players as  
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
        
        
        
        select * from vokigames.mm_2020_05_2021_07 limit 10
        
        alter table vokigames.mm_2020_05_2021_07  rename column activity_day to  activity_date
        
        
        WITH tmp_variables AS (
SELECT 
   '2021-07-01'::DATE AS StartDate, 
   '2021-07-30'::DATE AS FinishDate
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
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
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
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        )
        select 
        	activity_date,
        	count(distinct event_user) as dau,
        	sum(rev) as revenue
        from all_data
        group by 1 
        order by 1
        
        
        with temp as 
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
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = '2021-08-03' --between '2021-08-01' and '2021-08-03'
        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
        )
        select 
        	activity_day,
        	count(distinct event_user) as dau,
        	sum(rev) revenue
        from temp
        group by 1
        order by 1
        
        