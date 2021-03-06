
 create table vokigames.hs_2020_05_2021_07 as
 select * from vokigames.hs_amz_2020_05_2021_07
 union all
  select * from vokigames.hs_ios_2020_05_2021_07
 union all
  select * from vokigames.hs_gp_2020_05_2021_07
  
    alter table vokigames.hs_2020_05_2021_07
  rename column activity_day to activity_date
 
select * from vokigames.hs_2020_05_2021_07 limit 10

select max(activity_date) from vokigames.hs_2020_05_2021_07 --limit 10

select distinct device_region from vokigames.hs_2020_05_2021_07 --where device_region is null

select * from vokigavjuenmes.mm_2020_05_2021_07 limit 10

alter table  rename column activity_day to activity_date



insert into vokigames.hs_2020_05_2021_07
WITH tmp_variables AS (
		SELECT 
		   '2021-01-01'::DATE AS StartDate, 
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
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4698_pq sp
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
                 from impala_main_day.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4698_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 


insert into vokigames.hs_2020_05_2021_07
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
      	'gp' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4699_pq sp
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
                 from impala_main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  = (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4699_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between (SELECT FinishDate FROM tmp_variables) - interval '10 days' and (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 
        
        
        
        insert into vokigames.hs_2020_05_2021_07
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
      	from impala_main_day.seg_players_4700_pq sp
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
                 from impala_main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4700_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 


-- HS
      	      	create table vokigames.hs_amz_2020_05_2021_07 as
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
--      	from impala_main_day.seg_players_4698_pq sp
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
--                 from impala_main_day.valid_iap_4698_pq vi 
--                 where 
--                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--                 group by 1,2
--        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
--        left join 
--        (
--        select
--        *
--        from impala_main_day.seg_install_info_4698_pq si 
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
--      	from impala_main_day.seg_players_4699_pq sp
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
--                 from impala_main_day.valid_iap_4699_pq vi 
--                 where 
--                  timestamp 'epoch' + day::float * interval '1 second'  between '2020-04-30' and '2021-08-03'
--                 group by 1,2
--        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
--        left join 
--        (
--        select
--        *
--        from impala_main_day.seg_install_info_4699_pq si 
--        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between '2020-04-30' and '2021-08-03'
--        ) si on sp.event_user = si.event_user and sp.first_session = si.first_session
--        union all
      	      	
      	      	
      	      	      	WITH tmp_variables AS (
		SELECT 
		   '2021-08-01'::DATE AS StartDate, 
		   '2021-08-04'::DATE AS FinishDate
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
      	from impala_main_day.seg_players_4700_pq sp
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
                 from impala_main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_4700_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session / 1000 = si.first_session
        limit 10
        
        select * from impala_main_day.seg_players_4700_pq limit 10
        
        select * from impala_main_day.seg_install_info_4700_pq limit 10
        
        
        select
        *
        from impala_main_day.seg_install_info_4700_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = '2021-08-03'
        limit 10
        
        select max(activity_date)--distinct platform 
        from vokigames.hs_2020_05_2021_07 
--        where activity_date between '2021-02-01' and '2021-02-28' 
        
        
        delete from vokigames.hs_2020_05_2021_07  where activity_date >= '2021-08-05'
        
        
        -- HS REVENUE
        
                WITH tmp_variables AS (
		SELECT 
		   '2020-04-30'::DATE AS StartDate, 
		   '2021-08-29'::DATE AS FinishDate
		)
		,
		rev as
		(
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  'ios' as platform,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4698_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
         union all
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  'gp' as platform,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
                  union all
        select
--                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as ,
                  'amz' as platform,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_4700_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
         )
         select 
         	(timestamp 'epoch' + day::float * interval '1 second')::date report_period,
         	platform,
         	sum(rev)*.7,
         	sum(orders)
         from rev
         group by 1,2
         order by 1,2 desc
         
         
         -- HS DAU
         
         WITH tmp_variables AS (
		SELECT 
		   '2021-08-05'::DATE AS StartDate, 
		   '2021-08-12'::DATE AS FinishDate
		)
		select 
--      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
--      	sp.hour,
--      	rev.day,
--      	sp.event_user,
         (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
         count (distinct sp.event_user)
--        sp.payer,
--        rev.rev,
--        rev.orders,
--        si.device_region,
--        si.install_type,
--        si.device_type,
--      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_4698_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
      	and timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'  >= (SELECT StartDate FROM tmp_variables) 
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
		group by 1
		order by 1 desc
        
--      	left join (
--                select
--                  event_user,
--                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
--                  sum(offer_price) as rev,
--                  count(offer_price) as orders
--                 from impala_main_day.valid_iap_4698_pq vi 
--                 where 
--                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
--                 group by 1,2
--        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
--        left join 
--        (
--        select
--        *
--        from impala_main_day.seg_install_info_4698_pq si 
--        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between (SELECT FinishDate FROM tmp_variables) - interval '10 days' and (SELECT FinishDate FROM tmp_variables)
--        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session
        