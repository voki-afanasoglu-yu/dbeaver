--https://app.asana.com/0/347104365046376/1201168685574457/f

select 
	distinct event_user,
--	device_id,
	json_extract_path_text(parameters, 'attributes',  'abi')::text abi -- ,
--	json_extract_path_text(parameters, 'attributes', 'payer_type')::text payer
--	*
--	distinct event_type 
from vokigames.all_events_31756_pq 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-07-01' and '2021-09-30'
	and event_type = 'user'
	and event_user in (
	'06122020-144024-aJX59NbI',
	'17012021-073356-qw1I0sGH',
	'13052021-083854-ha71r5pl',
	'19022021-184954-cGGKJtjE',
	'02042021-184606-Ko5lpuNg'
	)
--limit 10

select 
--	distinct event_user,
--	device_id,
	distinct event_user,
	json_extract_path_text(parameters, 'attributes',  'abi')::text abi -- ,
--	json_extract_path_text(parameters, 'attributes', 'payer_type')::text payer
--	*
--	distinct event_type 
from vokigames.all_events_31756_pq 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30'
	and 
	event_type = 'user'
group by 1,2
limit 10

select * from vokigames.all_events_31756_pq limit 10

select * from impala_main_day.seg_users_31756_pq sup limit 10

select * from vokigames.seg_players_31756_pq sup limit 10

select * from vokigames.valid_iap_31756_pq vip limit 10

select 
json_extract_path_text(parameters, 'attributes',  'abi')::text abi,
* 
from vokigames.all_events_31756_pq 
where timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30'
and event_type = 'user'
and json_extract_path_text(parameters, 'attributes',  'abi')::text in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a')
limit 100

-- выручка

select 
	date_trunc('month', timestamp 'epoch' + day * interval '1 second')::date report_period,
	event_user,
	sum(offer_price)*.7 as rev
from vokigames.valid_iap_31756_pq vip 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30'
group by 1,2
order by 1
limit 10

-- активные пользователи

select 
	distinct event_user
from vokigames.seg_players_31756_pq
where timestamp 'epoch' + (hour-86400 ) * interval '1 second' between '2021-09-01' and '2021-09-30'
and timestamp 'epoch' + (last_active/ 1000) * interval '1 second' between '2021-09-01' and '2021-09-30'
limit 10

-- окончательный запрос

with android as 
(
select 
	distinct sp.event_user,
	abi,
	rev,
	report_period,
	LAST_VALUE(abi) over (partition by sp.event_user ) test 
from vokigames.seg_players_31756_pq sp
left join 
(
select 
	event_user,
	json_extract_path_text(parameters, 'attributes',  'abi')::text abi
from vokigames.all_events_31756_pq 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30'
	and event_type = 'user'
	and json_extract_path_text(parameters, 'attributes',  'abi')::text in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a')
group by 1,2
) abi using (event_user)
left join 
(
select 
	date_trunc('month', timestamp 'epoch' + day * interval '1 second')::date report_period,
	event_user,
	sum(offer_price)*.7 as rev
from vokigames.valid_iap_31756_pq vip 
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-01' and '2021-09-30'
group by 1,2
order by 1
) rev using (event_user)
where timestamp 'epoch' + (sp.hour-86400 ) * interval '1 second' between '2021-09-01' and '2021-09-30'
and timestamp 'epoch' + (sp.last_active/ 1000) * interval '1 second' between '2021-09-01' and '2021-09-30'
)
select *
from android 
where abi not in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a')
limit 200



select 	
	abi,
	count (distinct event_user) as active_users,
	sum(rev) as revenue
from android
group by 1



select 	--*
count (distinct event_user),
count (
case when  abi in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a') then abi
end
) abi,
count (
case when  abi in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a') then abi
end
) test
from android
where event_user in (select event_user from android where abi not in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a'))


select 	
	abi,
	count (distinct event_user) as active_users,
	sum(rev) as revenue
from android
group by 1



select *
from android 
where abi not in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a')
limit 200



select 	--*
count (distinct event_user),
count (
case when  abi in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a') then abi
end
)
from android
where event_user in (select event_user from android where abi not in ('x86', 'x86_64', 'armeabi-v7a', 'arm64-v8a'))
--limit 200



select 	
	abi,
	count (distinct event_user) as active_users,
	sum(rev) as revenue
from android
group by 1


