-- 2021.10.18

-- DAU comparison


-- all_events
select 
	timestamp 'epoch' + day * interval '1 second' report_period,
--	json_extract_path_text(payload, 'EventId')::text as event_id,
	count (distinct event_user) as ext_user
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30' 
    and event_type = 'event'
group by 1
order by 1

-- attempts
select 
	timestamp 'epoch' + day * interval '1 second' report_period,
--	json_extract_path_text(payload, 'EventId')::text as event_id,
	count (distinct event_user) as ext_user
from
    main_day.attempts_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30' 
--    and event_type = 'event'
group by 1
order by 1


-- seg_players


select
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' report_period,
    count(event_user) as dau_all
--    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
--    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic
from
    impala_main_day.seg_players_30975_pq sp
--        left join
--    main_day.seg_install_info_30975_pq si
--        on
--           ( sp.event_user = si.event_user and timestamp 'epoch' + (si.hour - 86400) * interval '1 second' = '2021-09-30')
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-09-01' and '2021-09-30'
    and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1


-- seq players last active 

select
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' report_period,
--    date_trunc('day', timestamp 'epoch' + (last_active/1000)::float * interval '1 second') report_period,
    count(event_user) as dau_all
--    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
--    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic
from
    main_day.seg_players_30975_pq sp
--        left join
--    main_day.seg_install_info_30975_pq si
--        on
--           ( sp.event_user = si.event_user and timestamp 'epoch' + (si.hour - 86400) * interval '1 second' = '2021-09-30')
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-09-01' and '2021-09-30'
    and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1


select
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' report_period,
    'ios' as platform,
    count(sp.event_user) as dau_all,
    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic
from
    impala_main_day.seg_players_30975_pq sp
        left join
    impala_main_day.seg_install_info_30975_pq si
        on
           ( sp.event_user = si.event_user  and sp.hour = si.hour)
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-09-01' and '2021-09-30'
    and timestamp 'epoch' + (si.hour - 86400) * interval '1 second' between '2021-09-01' and '2021-09-30'
    and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1


-- ???????? ????????? ios
select
    (timestamp 'epoch' + (sp.hour - 86400) * interval '1 second')::date report_period,
    'ios' as platform,
    count(distinct sp.event_user) as dau_all,
    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic,
    sum(rev) as rev
from
    impala_main_day.seg_players_30975_pq sp
        left join
    impala_main_day.seg_install_info_30975_pq si
        on
           ( sp.event_user = si.event_user  and sp.hour = si.hour)
    left join 
    (
    select 
    	day,
		event_user,
		sum(offer_price)*.7 as rev
	from impala_main_day.valid_iap_30975_pq vip
	where
    timestamp 'epoch' + (day) * interval '1 second' between '2021-01-01' and '2021-09-30'
    group by 1,2
    ) rev on (sp.event_user = rev.event_user  and sp.hour-86400 = rev.day)
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-01-01' and '2021-09-30'
    and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1


select 
	day,
	event_user,
	offer_price
from impala_main_day.valid_iap_30975_pq vip limit 10


-- ??? ????????? ios / gp

select
    (timestamp 'epoch' + (sp.hour - 86400) * interval '1 second')::date report_period,
    'ios' as platform,
    count(distinct sp.event_user) as dau_all,
    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic,
    sum(rev) as rev,
    sum(case when si.install_type = 'paid' then rev end) rev_paid,
    sum(case when si.install_type <> 'paid' then rev end) rev_organic
from
    impala_main_day.seg_players_30975_pq sp
        left join
    impala_main_day.seg_install_info_30975_pq si
        on
           ( sp.event_user = si.event_user  and sp.hour = si.hour)
    left join 
    (
    select 
    	day,
		event_user,
		sum(offer_price)*.7 as rev
	from impala_main_day.valid_iap_30975_pq vip
	where
    timestamp 'epoch' + (day) * interval '1 second' between '2021-01-01' and '2021-09-30'
    group by 1,2
    ) rev on (sp.event_user = rev.event_user  and sp.hour-86400 = rev.day)
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-01-01' and '2021-09-30'
    and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
union all
select
    (timestamp 'epoch' + (sp.hour - 86400) * interval '1 second')::date report_period,
    'gp' as platform,
    count(distinct sp.event_user) as dau_all,
    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic,
    sum(rev) as rev,
    sum(case when si.install_type = 'paid' then rev end) rev_paid,
    sum(case when si.install_type <> 'paid' then rev end) rev_organic
from
    impala_main_day.seg_players_31756_pq sp
        left join
    impala_main_day.seg_install_info_31756_pq si
        on
           ( sp.event_user = si.event_user  and sp.hour = si.hour)
      	left join 
    (
    select 
    	day,
		event_user,
		sum(offer_price)*.7 as rev
	from impala_main_day.valid_iap_31756_pq vip
	where
    timestamp 'epoch' + (day) * interval '1 second' between '2021-01-01' and '2021-09-30'
    group by 1,2
    ) rev on (sp.event_user = rev.event_user  and sp.hour-86400 = rev.day)
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-01-01' and '2021-09-30'
    and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1 




select
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' report_period,
    'gp' as platform,
    count(event_user) as dau_all
--    sum(case when si.install_type = 'paid' then 1 else 0 end) dau_paid,
--    sum(case when si.install_type = 'paid' then 0 else 1 end) dau_organic
from
    main_day.seg_players_30975_pq sp
--        left join
--    main_day.seg_install_info_30975_pq si
--        on
--           ( sp.event_user = si.event_user and timestamp 'epoch' + (si.hour - 86400) * interval '1 second' = '2021-09-30')
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-01-01' and '2021-10-17'
    and timestamp 'epoch' + (last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
group by 
    1
order by 1


invalidate metadata vokigames.seg_install_info_30975_pq

REFRESH vokigames.seg_install_info_30975_pq

unix_timestamp

select *
from
    main_day.seg_players_30975_pq sp
where
    timestamp 'epoch' + (sp.hour - 86400) * interval '1 second' between '2021-09-01' and '2021-09-30'
limit 10    
    
 
    select 
--    vivf max(timestamp 'epoch' + (hour - 86400) * interval '1 second'),
    timestamp 'epoch' + (hour - 86400) * interval '1 second',
    *
from
    main_day.seg_install_info_30975_pq
--where
--    timestamp 'epoch' + (hour - 86400) * interval '1 second' = '2021-10-10'  --between '2021-10-01' and '2021-10-10'
order by 1 desc
limit 10    
    
    


-- DAU MM

create table vokigames.mm_dau_2021 as
with dau as
(
select 
	activity_date::date,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.mm_2020_05_2021_07 
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
		avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30
from dau
order by 1

select * from vokigames.mm_dau_2021 limit 10

-- HS

create table vokigames.hs_dau_2021 as
with dau as
(
select 
	activity_date::date,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.hs_2020_05_2021_07 
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
		avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30
from dau
order by 1


-- GS

create table vokigames.gs_dau_2021 as
with dau as
(
select 
	activity_date::date,
	count(distinct event_user) as dau,
	sum(rev)*.7 as revenue,
	sum(rev) * 0.7 / count(distinct event_user) as arpdau
from vokigames.gs_2020_05_2021_07 
group by 1
)
select 
	*,
	avg(dau) over (order by activity_date rows 89 preceding) as avg_dau_90,
	avg(revenue) over (order by activity_date rows 89 preceding) as avg_rev_90,
	avg(arpdau) over (order by activity_date rows 89 preceding) as avg_arp_dau_90,
		avg(dau) over (order by activity_date rows 29 preceding) as avg_dau_30,
	avg(revenue) over (order by activity_date rows 29 preceding) as avg_rev_30,
	avg(arpdau) over (order by activity_date rows 29 preceding) as avg_arp_dau_30
from dau
order by 1

select * from vokigames.gs_dau_2021 limit 10

select min(activity_date), max(activity_date) from vokigames.gs_dau_2021 limit 10


select 
	activity_date::date,
	count(distinct case when platform = 'ios' then event_user ELSE NULL end) as ios_dau,
	count(distinct case when platform = 'gp' then event_user ELSE NULL end) as gp_dau,
	count(distinct case when platform = 'amz' then event_user ELSE NULL end) as amz_dau
from vokigames.mm_2020_05_2021_07 
group by 1
order by 1


-- ARPPU, CR, ???? ????????

-- MM

create table vokigames.mm_main_metrics_2021 as
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
from vokigames.mm_cohorts_2021 
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



-- MM2

create table vokigames.mm_main_metrics_2021 as

drop table vokigames.mm_main_metrics_2021 

create table vokigames.mm_main_metrics_2021 as


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



select * from vokigames.mm_2020_05_2021_07 limit 10

select 
activity_date,
sum(rev * .7) as revenue
from 
vokigames.mm_2020_05_2021_07
where activity_date >= '2021-08-01'
group by 1
order by 1

select 
activity_date,
sum(rev * .7) as revenue
from 
vokigames.mm_cohorts_2021
where activity_date >= '2021-08-01'
group by 1
order by 1

-- MM cohorts

select * from vokigames.mm_2020_05_2021_07 limit 10


drop table vokigames.mm_cohorts_2021

create table vokigames.mm_cohorts_2021 as
with cohort as
(
select 
	first_session_date::date,
--	activity_date::date,
	(activity_date::date - first_session_date::date)::int  as lt_period,
--	'total' platform,
	count(distinct event_user) as cohort_size_period,
	sum(rev * .7) as revenue
	from vokigames.mm_2020_05_2021_07
	where first_session_date >= '2020-04-30'
	group by 1,2
	--	order by 1 desc,2
)
select 
	*,
	max (cohort_size_period) over (partition by first_session_date ) as cohort_size,
	cohort_size_period * 1.0 / max(cohort_size_period) over (partition by first_session_date ) as rr,
	revenue /  max(cohort_size_period) over (partition by first_session_date ) as arpu
--	sum(revenue /  max (cohort_size_period) over (partition by first_session_date )) over (partition by first_session_date order by lt_period rows unbounded preceding)
	from cohort
	order by 1,2
	
union all

with cohort as
(
	select 
	first_session_date::date,
	platform,
	(activity_date::date - first_session_date::date)::int  as lt_period,
	count(distinct event_user) as cohort_size_period,
	sum(rev * .7) as revenue
	from vokigames.mm_2020_05_2021_07
	where first_session_date >= '2020-07-30'
	group by 1,2,3
)
	select 
	*,
	first_value (cohort_size_period) over (partition by first_session_date, platform ) as cohort_size,
	first_value(cohort_size_period) over (partition by first_session_date ) as cohort_size,
	cohort_size_period * 1.0 / first_value(cohort_size_period) over (partition by first_session_date ) as rr,
	revenue /  first_value(cohort_size_period) over (partition by first_session_date ) as arpu
--	sum(revenue /  max (cohort_size_period) over (partition by first_session_date )) over (partition by first_session_date order by lt_period rows unbounded preceding)
	from cohort
	order by 1,3,2
	
	
	-- gs cohorts
	
	
	create table vokigames.gs_cohorts_2021 as
	with cohort as
(
select 
	first_session_date::date,
--	activity_date::date,
	(activity_date::date - first_session_date::date)::int  as lt_period,
--	'total' platform,
	count(distinct event_user) as cohort_size_period,
	sum(rev * .7) as revenue
	from vokigames.gs_2020_05_2021_07
	where first_session_date >= '2020-04-30'
	group by 1,2
	--	order by 1 desc,2
)
select 
	*,
	max (cohort_size_period) over (partition by first_session_date ) as cohort_size,
--	max(cohort_size_period) over (partition by first_session_date ) as cohort_size,
	cohort_size_period * 1.0 / max(cohort_size_period) over (partition by first_session_date ) as rr,
	revenue /  max(cohort_size_period) over (partition by first_session_date ) as arpu
--	sum(revenue /  max (cohort_size_period) over (partition by first_session_date )) over (partition by first_session_date order by lt_period rows unbounded preceding)
	from cohort
	order by 1,2
	
	
	-- hs cohorts
	
	
	create table vokigames.hs_cohorts_2021 as
	with cohort as
(
select 
	first_session_date::date,
--	activity_date::date,
	(activity_date::date - first_session_date::date)::int  as lt_period,
--	'total' platform,
	count(distinct event_user) as cohort_size_period,
	sum(rev * .7) as revenue
	from vokigames.hs_2020_05_2021_07
	where first_session_date >= '2020-04-30'
	group by 1,2
	--	order by 1 desc,2
)
select 
	*,
	max (cohort_size_period) over (partition by first_session_date ) as cohort_size,
--	max(cohort_size_period) over (partition by first_session_date ) as cohort_size,
	cohort_size_period * 1.0 / max(cohort_size_period) over (partition by first_session_date ) as rr,
	revenue /  max(cohort_size_period) over (partition by first_session_date ) as arpu
--	sum(revenue /  max (cohort_size_period) over (partition by first_session_date )) over (partition by first_session_date order by lt_period rows unbounded preceding)
	from cohort
	order by 1,2


-- HS
create table vokigames.hs_main_metrics_2021 as


create table vokigames.hs_main_metrics_2021_source as
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


-- hs2

insert into vokigames.hs_main_metrics_2021
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
where activity_date >= '2021-08-05'
group by 1


--hs 3


create table vokigames.hs_main_metrics_2021 as
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
from vokigames.hs_main_metrics_2021_source
order by 1



create table vokigames.hs_main_metrics_2021 as
select 
	activity_date,
	project,
	dau,
	revenue,
	payers,
	payers_share,
	arppdau,
	arpdau
from vokigames.hs_main_metrics_2021_old 
where activity_date < '2021-08-05'

select * from vokigames.hs_main_metrics_2021



select * from vokigames.hs_main_metrics_2021 limit 10

update vokigames.hs_main_metrics_2021 set project = 'hs' where project = 'mm'


-- GS

create table vokigames.gs_main_metrics_2021 as
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

insert into insert into vokigames.gs_main_metrics_2021(project) values('gs')

update vokigames.gs_main_metrics_2021 set project = 'gs' where project = 'mm'

select * from  vokigames.gs_main_metrics_2021 where project = 'gs' and activity_date is null limit 10

