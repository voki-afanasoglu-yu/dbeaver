--https://app.asana.com/0/0/1201113043689697/f

-- DAU

create table vokigames.dau_karl_bedroom as
select 
	(timestamp 'epoch' + day * interval '1 second')::date report_date,
	'ios' as platform,
	count (distinct event_user) as DAU,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as DAU_new
from
	vokigames.attempts_30975_pq
	left join vokigames.seg_players_30975_pq using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1
union all
select 
	(timestamp 'epoch' + day * interval '1 second')::date report_date,
	'gp' as platform,
	count (distinct event_user) as DAU,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as DAU_new
from
	vokigames.attempts_31756_pq
	left join vokigames.seg_players_31756_pq using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1
order by 
	1
	
	
-- DAU + Revenue + payers

drop table 	vokigames.dau_karl_bedroom
	
create table vokigames.dau_karl_bedroom as
with dau_ios as 
(
select 
	(timestamp 'epoch' + day * interval '1 second')::date report_date,
	'ios' as platform,
	count (distinct event_user) as DAU,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as DAU_new
from
	vokigames.attempts_30975_pq att
	left join vokigames.seg_players_30975_pq sp using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1
order by 
	1
)	
,
dau_gp as 
(
select 
	(timestamp 'epoch' + day * interval '1 second')::date report_date,
	'gp' as platform,
	count (distinct event_user) as DAU,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as DAU_new
from
	vokigames.attempts_31756_pq att
	left join vokigames.seg_players_31756_pq sp using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1
order by 
	1
)
select 
	*,
	dau_new * 1.0 / dau dau_share,
	payers_new * 1.0 / payers payers_share,
	rev_new / rev rev_share
from dau_ios
left join 
(
	select 
	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
--	event_user,
	sum(offer_price)*.7 as rev,
	sum(
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then (offer_price)*.7
	end
	) as rev_new,
	count(distinct event_user) as payers,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as payers_new
from 
	vokigames.valid_iap_30975_pq
	left join vokigames.seg_players_30975_pq sp using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (sp.hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1--,2
) via on dau_ios.report_date = via.report_date
union all
select 
	*,
	dau_new * 1.0 / dau dau_share,
	payers_new * 1.0 / payers payers_share,
	rev_new / rev rev_share
from dau_gp
left join 
(
	select 
	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
--	event_user,
	sum(offer_price)*.7 as rev,
	sum(
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then (offer_price)*.7
	end
	) as rev_new,
	count(distinct event_user) as payers,
	count (distinct 
	case
	when timestamp 'epoch' + (first_session/1000)::float * interval '1 second' > '2021-04-26' then event_user
	end
	) as payers_new
from 
	vokigames.valid_iap_31756_pq
	left join vokigames.seg_players_31756_pq sp using (event_user)
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-27' and '2021-10-10'
	and timestamp 'epoch' + (sp.hour-86400) * interval '1 second' = '2021-10-10'
group by 
	1--,2
) via on dau_gp.report_date = via.report_date

grant select on vokigames.dau_karl_bedroom to group analytics


	
select 
	report_date,
	sum(dau) dau,
	sum(dau_new) dau_new
from vokigames.dau_karl_bedroom
group by 1
order by 1


select 
distinct(json_extract_path_text(payload, 'product_id')::text) product_id 
from vokigames.valid_iap_30975_pq vip 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26'
order by 1
--limit 10

select 
	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
	event_user,
	product_id ,
	sum(offer_price)*.7 as rev
from 
	vokigames.valid_iap_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26'
	and product_id like '%extension%'
group by 
	1,2,3
order by 
	1
--limit 10
	
	
select distinct product_id 
from vokigames.valid_iap_30975_pq

select * from vokigames.attempts_30975_pq limit 10

select * from vokigames.all_events_30975_pq 
where event_type = 'event'
and parameters like '%iap.purchase%'
--and payload like '%"EventId":"KarlBedroom"%'
limit 10

select * from vokigames.seg_users_30975_pq limit 10

select * from vokigames.seg_players_30975_pq 
where level_conv is not null 
limit 10


-- ?????? ????????????? ??????? ?? ?????????? ??????? ???????

-- Manor_RoomDesign_extension

select 
	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
	count (distinct event_user) players
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26' 
    and event_type = 'event'
    and parameters like '%"RoomdesignEvent.EventStarted"%'
    and payload like '%"EventId":"KarlBedroom"%'
group by 
	1
order  by 
	1
	
-- ???????? ????????????? ? ???????

select 
	report_period,
--	event_user,
--	product_id,
	sum(rev) as rev
from
(	
	select 
	distinct event_user as event_user
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26' 
    and event_type = 'event'
    and parameters like '%"RoomdesignEvent.EventStarted"%'
    and payload like '%"EventId":"KarlBedroom"%'
) users
left join 
(
select
	timestamp 'epoch' + day * interval '1 second' report_period,
	event_user,
	product_id,
	sum(offer_price)*.7 rev 
from vokigames.valid_iap_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-10-26' 
group by 1,2,3
) rev using (event_user)
group by 1--,2,3
order by 1
--limit 10


-- ??????? RoomdesignEvent.Extension

	select
	timestamp 'epoch' + day * interval '1 second' report_period,
	json_extract_path_text(payload, 'EventId')::text as event_id,
	json_extract_path_text(parameters, 'name')::text as event_name,
	count (event_user) as ext_user
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-10-26' 
    and event_type = 'event'
    and parameters like '%"RoomdesignEvent%'
--    and payload like '%"EventId":"KarlBedroom"%'
group by 1,2,3
order by 1


	
select 
--	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
	count (distinct event_user) players
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26' 
    and event_type = 'event'
    and parameters like '%"RoomdesignEvent%' --.EventStarted"%'
    and payload like '%"EventId":"KarlBedroom"%'
--group by 
--	1
--order  by 
--	1
    
    select *
--	(timestamp 'epoch' + day * interval '1 second')::date as report_date,
--	count (distinct event_user) players
from
    main_day.all_events_30975_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-04-15' and '2021-04-26' 
    and event_type = 'event'
    and parameters like '%"RoomdesignEvent.EventStarted"%'
limit 10

-- ?????? ?? ??????? ?? ????? ?? ??????? ????? ? ?????? 2021

select * 
--	distinct test_id 
from impala_main_day.abtests_30975_pq 
where abgroup  in ('ab_CarlsBedroom_NoEvent_control' , 'ab_CarlsBedroom_MainBalance_test_a', 'ab_CarlsBedroom_MainBalanceNoX2_test_b')
limit 10


with users as 
(
select 	
	abgroup,
	count (distinct event_user),
	count (distinct 
	case when conv.event_time between '2021-04-15' and '2021-04-26' then event_user 
	end 
	) as payers_conv
from impala_main_day.abtests_31756_pq 
left join
	(
	select 
	distinct event_user,
	timestamp 'epoch' + (event_time/1000)  * interval '1 second' event_time 
--	count (distinct event_user )
from impala_main_day.all_events_31756_pq 
where 
	event_type = 'event'
	and parameters like '%"Conversion.Conversion"%'
	and timestamp 'epoch' + (event_time/1000)  * interval '1 second' between '2021-04-15' and '2021-04-26'
	and timestamp 'epoch' + day  * interval '1 second' between '2021-04-15' and '2021-04-26'
	) as conv using (event_user)
where abgroup  in ('ab_CarlsBedroom_NoEvent_control' , 'ab_CarlsBedroom_MainBalance_test_a', 'ab_CarlsBedroom_MainBalanceNoX2_test_b')
group by 1 
)
select 
	*,
	payers_conv * 1.0 / count as conv_share
from users
	
	
	select *
--	distinct event_user,
--	timestamp 'epoch' + (event_time/1000)  * interval '1 second' event_time 
--	count (distinct event_user )
from impala_main_day.all_events_31756_pq 
where 
	event_type = 'event'
	and parameters like '%"Conversion.Conversion"%'
	and timestamp 'epoch' + (event_time/1000)  * interval '1 second' between '2021-04-15' and '2021-04-26'
	and timestamp 'epoch' + day  * interval '1 second' between '2021-04-15' and '2021-04-26'
limit 10


-- ??????? ??????????? ??????????





	
