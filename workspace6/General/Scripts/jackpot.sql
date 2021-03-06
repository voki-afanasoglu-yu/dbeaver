--https://app.asana.com/0/0/1200563890845828/f

select --*
payload,
 event_time,
  day,
  event_user,
  device_id,
  json_extract_path_text(payload, 'Bonus')::int bonus,
  json_extract_path_text(payload, 'TotalDays')::int TotalDays,
  json_extract_path_text(payload, 'TotalBonuses')::int TotalBonuses,
  json_extract_path_text(parameters, 'name') event_name,
  rank() over (partition by event_user order by event_time) as rank,
  json_extract_path_text(payload, 'TotalBonuses')::int - coalesce(lag(json_extract_path_text(payload, 'TotalBonuses')::int) over (partition by event_user order by event_time), 0) as diff
from main_day.all_events_30975_pq
where
    event_type = 'event'
    and timestamp 'epoch' + day * interval '1 second' between '2021-06-24' and '2021-09-24'
    and parameters like '%"DailyBonus.%'
    and payload like '%"Bonus":"9"%'
-- order by event_user
 limit 10
 

 -- ????? ????????? ????????????? ? ???????
 
 with users as
 (
 select --*
 event_time,
  day,
  event_user,
  device_id,
  json_extract_path_text(payload, 'Bonus')::int bonus,
  json_extract_path_text(payload, 'TotalDays')::int TotalDays,
  json_extract_path_text(payload, 'TotalBonuses')::int TotalBonuses,
  json_extract_path_text(parameters, 'name') event_name,
  rank() over (partition by event_user order by event_time) as rank
from main_day.all_events_30975_pq
where
    event_type = 'event'
    and timestamp 'epoch' + day * interval '1 second' between '2021-06-24' and '2021-09-24'
    and parameters like '%"DailyBonus.%'
    and payload like '%"Bonus":%'
 order by event_user
-- limit 10
 )
 select 
 	count (distinct event_user) users,
 	count ( 
 	case 
 	when bonus = 9 then event_user
 	end 
 	) jackpots,
 	count (event_user) as dailybonuses
 from users
 	

 -- ???????????? ? ???????? ??????? ? ???????? ?????????
 
  with users as
 (
 select --*
-- event_time,
 timestamp 'epoch' + (event_time / 1000)::float * interval '1 second' event_time,
  day,
  event_user,
  device_id,
  json_extract_path_text(payload, 'Bonus')::int bonus,
  json_extract_path_text(payload, 'TotalDays')::int TotalDays,
  json_extract_path_text(payload, 'TotalBonuses')::int TotalBonuses,
  json_extract_path_text(parameters, 'name') event_name,
  rank() over (partition by event_user order by event_time) as rank,
   json_extract_path_text(payload, 'TotalBonuses')::int - coalesce(lag(json_extract_path_text(payload, 'TotalBonuses')::int) over (partition by event_user order by event_time), 0) as diff
from main_day.all_events_30975_pq
where
    event_type = 'event'
    and timestamp 'epoch' + day * interval '1 second' between '2021-06-24' and '2021-09-24'
    and parameters like '%"DailyBonus.%'
    and payload like '%"Bonus":%'
-- order by event_user
-- limit 10
 )
 select 
*
 from users
 where event_user in (select event_user from users where (bonus = 9 and diff = 0))
 
 event_time,
  day,
  event_user,
  device_id,
  json_extract_path_text(payload, 'Difficulty')::int Difficulty,
  json_extract_path_text(payload, 'Rank')::int Difficulty,
  json_extract_path_text(payload, 'Mode') "Mode",
  json_extract_path_text(payload, 'Level')::int "Level",
  json_extract_path_text(payload, 'PackId') "PackId",
  json_extract_path_text(payload, 'AtScene')::int "AtScene",
  json_extract_path_text(payload, 'LastQuestCompleted')::int "LastQuestCompleted",
  json_extract_path_text(payload, 'Attempt')::int "Attempt",
  json_extract_path_text(payload, 'EndLevel')::int "EndLevel"
--  *
  from main_day.all_events_30975_pq 
  where event_type = 'event'
  and timestamp 'epoch' + day * interval '1 second' between '2021-06-26' and '2021-09-26'
  and parameters like '%"Purchase.Boosts"%'