 create table vokigames.gs_2020_05_2021_07 as
 select * from vokigames.gs_amz_2020_05_2021_07
 union all
  select * from vokigames.gs_ios_2020_05_2021_07
 union all
  select * from vokigames.gs_gp_2020_05_2021_07      
  
  alter table vokigames.gs_2020_05_2021_07
  rename column activity_day to activity_date
  
  select distinct device_region from vokigames.gs_2020_05_2021_07
  
  select max(activity_date) from vokigames.gs_2020_05_2021_07
  
  
  
        insert into vokigames.gs_2020_05_2021_07
		WITH tmp_variables AS (
		SELECT 
		   '2021-08-05'::DATE AS StartDate, 
		   '2021-08-12'::DATE AS FinishDate
		)
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
                 from impala_main_day.valid_iap_3789_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_3789_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 
        
        
        
                insert into vokigames.gs_2020_05_2021_07
		WITH tmp_variables AS (
		SELECT 
		   '2021-08-05'::DATE AS StartDate, 
		   '2021-08-12'::DATE AS FinishDate
		)
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
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
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
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_3790_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session
        
        
		insert into vokigames.gs_2020_05_2021_07
		WITH tmp_variables AS (
		SELECT 
		   '2021-08-13'::DATE AS StartDate, 
		   '2021-08-29'::DATE AS FinishDate
		)
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
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session
      	

create table vokigames.gs_amz_2020_05_2021_07 as
--      	select 
--      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
--      	sp.hour,
--      	rev.day,
--      	sp.event_user,
--         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
--        sp.payer,
--        rev.rev,
--        rev.orders,
--        si.device_region,
--        si.install_type,
--        si.device_type,
--      	'ios' as platform
--      	from 
--      	(
--      	select 
--      	*
--      	from impala_main_day.seg_players_3789_pq sp
--      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
--        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
--        ) as sp
--      	left join (
--                select
--                  event_user,
--                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
--                  sum(offer_price) as rev,
--                  count(offer_price) as orders
--                 from impala_main_day.valid_iap_3789_pq vi 
--                 where 
--                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--                 group by 1,2
--        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
--        left join 
--        (
--        select
--        *
--        from impala_main_day.seg_install_info_3789_pq si 
--        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
--        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
--        union all
--              	select 
--      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
--      	sp.hour,
--      	rev.day,
--      	sp.event_user,
--         timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second' as first_session_date,
--        sp.payer,
--        rev.rev,
--        rev.orders,
--        si.device_region,
--        si.install_type,
--        si.device_type,
--      	'gp' as platform
--      	from 
--      	(
--      	select 
--      	*
--      	from impala_main_day.seg_players_3790_pq sp
--      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
--        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
--        ) as sp
--      	left join (
--                select
--                  event_user,
--                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
--                  sum(offer_price) as rev,
--                  count(offer_price) as orders
--                 from impala_main_day.valid_iap_3790_pq vi 
--                 where 
--                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--                 group by 1,2
--        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
--        left join 
--        (
--        select
--        *
--        from impala_main_day.seg_install_info_3790_pq si 
--        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
--        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
--        union all
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
        
        
        
         delete from vokigames.gs_2020_05_2021_07  where activity_date >= '2021-08-05'
         
        select max(activity_date)--distinct platform 
        from vokigames.gs_2020_05_2021_07 
        
        
        WITH tmp_variables AS (
		SELECT 
		   '2021-08-01'::DATE AS StartDate, 
		   '2021-08-29'::DATE AS FinishDate
		)
		,
		rev as
		(
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3790_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
         union all
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3789_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
                  union all
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_3791_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
         )
         select 
         	 timestamp 'epoch' + day::float * interval '1 second',
         	sum(rev)*.7,
         	sum(orders)
         from rev
         group by 1
         order by 1
         
         
          
                 