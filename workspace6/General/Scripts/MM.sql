drop table vokigames.mm_cohorts_2021

create table vokigames.mm_cohorts_2021 as


alter table vokigames.mm_cohorts_2021 rename column  activity_day to activity_date


		insert into vokigames.mm_cohorts_2021 
		
		
		WITH tmp_variables AS (
		SELECT 
		   '2021-08-04'::DATE AS StartDate, 
		   '2021-08-12'::DATE AS FinishDate
		)
		    select 
		    	timestamp 'epoch' + day::float * interval '1 second' as day,
		    	sum (rev) * .7,
		    	sum (rev)
		    	from
			(
			select
                  event_user,
                  day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
		)
		group by 1
		order by 1
		
		
		select max(activity_date) 
		from vokigames.mm_2020_05_2021_07 --limit 10
                 
                 
                 
		drop table vokigames.test_mm_08 
		
		create table vokigames.test_mm_08 as
		
		insert into vokigames.mm_2020_05_2021_07 
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
      	'ios' as platform
      	from 
      	(
      	select 
      	*
      	from impala_main_day.seg_players_30975_pq sp
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
                 from impala_main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 
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
      	from impala_main_day.seg_players_31756_pq sp
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
                 from impala_main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_31756_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 
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
      	from impala_main_day.seg_players_31761_pq sp
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
                 from impala_main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join 
        (
        select
        *
        from impala_main_day.seg_install_info_31761_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' = (SELECT FinishDate FROM tmp_variables)
        ) si on sp.event_user = si.event_user and sp.first_session/1000 = si.first_session 
        
        
        select --*
        max(activity_date)--, min(activity_date)
        from vokigames.mm_2020_05_2021_07 --where activity_date >= '2021-08-05'
--        limit 10
        
        
        select max(timestamp 'epoch' + (hour - 86400)::float * interval '1 second') from vokigames.seg_install_info_30975_pq limit 10
        
        
        
        select
        activity_date,
        sum(rev)*.7
        from vokigames.mm_2020_05_2021_07 
        where activity_date >= '2021-08-01' --and platform = 'amz'
        group by 1
        order by 1
        
        
        
        select --*
        max(activity_date), min(activity_date)
        from vokigames.mm_2020_05_2021_07 --where activity_date >= '2021-08-05'
--        limit 10
        
        
        
        
        select --distinct device_region
--        min(activity_day), 
        max(activity_day)
        from vokigames.mm_cohorts_2021
        
--        limit 10
        
        select * from impala_main_day.seg_players_31756_pq sp  
        where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  = '2021-08-08'
        limit 10
        
        select max(timestamp 'epoch' + (hour)::float * interval '1 second') from main_day.seg_install_info_31761_pq limit 10
        
        
 create table vokigames.main_metrics_2021 as       
        (
with dau as
(
select 
	activity_date::date::text,
	'mm' as project,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as payers,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) * 1.0 / count(distinct event_user) as payers_share,	
	sum(rev) * 0.7 / 	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as arppdau,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.mm_2020_05_2021_07
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
	avg(arppdau) over (order by activity_date rows 89 preceding) as avg_arpp_dau_90,
	avg(payers_share) over (order by activity_date rows 89 preceding) as avg_payers_share_90,
	avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30,
	avg(arppdau) over (order by activity_date rows 29 preceding) as avg_arpp_dau_30,
	avg(payers_share) over (order by activity_date rows 29 preceding) as avg_payers_share_30
from dau
order by 1
)
union all
(
with dau as
(
select 
	activity_date::date::text,
	'hs' as project,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as payers,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) * 1.0 / count(distinct event_user) as payers_share,	
	sum(rev) * 0.7 / 	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as arppdau,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.hs_2020_05_2021_07
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
	avg(arppdau) over (order by activity_date rows 89 preceding) as avg_arpp_dau_90,
	avg(payers_share) over (order by activity_date rows 89 preceding) as avg_payers_share_90,
	avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30,
	avg(arppdau) over (order by activity_date rows 29 preceding) as avg_arpp_dau_30,
	avg(payers_share) over (order by activity_date rows 29 preceding) as avg_payers_share_30
from dau
order by 1
)
union all
(
with dau as
(
select 
	activity_date::date::text,
	'gs' as project,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as payers,
	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) * 1.0 / count(distinct event_user) as payers_share,	
	sum(rev) * 0.7 / 	count( distinct 
	case
	when rev > 0 then event_user 
	end
	) as arppdau,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.gs_2020_05_2021_07
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
	avg(arppdau) over (order by activity_date rows 89 preceding) as avg_arpp_dau_90,
	avg(payers_share) over (order by activity_date rows 89 preceding) as avg_payers_share_90,
	avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30,
	avg(arppdau) over (order by activity_date rows 29 preceding) as avg_arpp_dau_30,
	avg(payers_share) over (order by activity_date rows 29 preceding) as avg_payers_share_30
from dau
order by 1
)
        

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
                  'ios' as platform,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_30975_pq vi 
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
                 from impala_main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
                  union all
        select
--                  event_user,
                          day, -- timestamp 'epoch' + day::float * interval '1 second' as 
                  'amz' as platform,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from impala_main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
                 group by 1--,2
         )
         select 
         	timestamp 'epoch' + day::float * interval '1 second',
--         	platform,
         	sum(rev),
         	sum(orders)
         from rev
         group by 1--,2
         order by 1--,2
         
         
         -- MM New users
         
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
      	from impala_main_day.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
      	and timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'  >= (SELECT StartDate FROM tmp_variables) 
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
		group by 1
		order by 1 desc
		
		
		         -- MM DAU
         
                  WITH tmp_variables AS (
		SELECT 
		   '2021-01-01'::DATE AS StartDate, 
		   '2021-08-12'::DATE AS FinishDate
		)
		select 
      	timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' as activity_day,
--      	sp.hour,
--      	rev.day,
--      	sp.event_user,
--         (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
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
      	from impala_main_day.seg_players_30975_pq sp
      	where timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
--      	and timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'  >= (SELECT StartDate FROM tmp_variables) 
		and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        ) as sp
		group by 1
		order by 1 desc
        
        