--https://app.asana.com/0/1200467786415640/1201007216243331/f


select 
	json_extract_path_text(parameters , 'item'),
	* 
from main_day.all_events_30975_pq 
where event_type = 'purchase'
--and parameters like '%misc.currency_given.coins.Racing%'
-- and regexp_extract(parameters , '"item":"(\\\w+)"', 1) like '%"misc.boosters%'
and timestamp 'epoch' + (day) * interval '1 second' between '2021-06-18' and '2021-09-28'
and json_extract_path_text(parameters , 'item') like '%misc.currency_given.coins.Racing%'
limit 10


 select * from main_day.attempts_30975_pq limit 10
 
 select * from main_day.all_events_30975_pq limit 10
        
        with all_levels as
        (
        select 
            event_user,
    		max(level) - min(level) as all_levels
            from main_day.attempts_30975_pq  -- limit 10
            where (name = 'LevelComplete' or name = 'LevelFailed')
            and day between unix_timestamp('2021-05-18')  and unix_timestamp('2021-05-28')
            group by 1
        )
        ,
        screen_levels as
        (
        select 
                event_user,
                count (distinct cast(regexp_extract(payload , '"Level":"(\\\d+)"', 1) as int)) as Screen_Levels
        from 
            main_day.all_events_30975_pq 
        where 
            day >= unix_timestamp('2021-05-18') 
            and day <= unix_timestamp('2021-05-28')
            and event_type = 'event'
            and (
            parameters like '%"ISPY.LevelComplete"%' 
            or parameters like '%"ISPY.LevelFailed"%'
            )
            and cast(regexp_extract(payload , '"MakeScreenshot":"(\\\d+)"', 1) as int) > 0
            and cast(regexp_extract(payload , '"UsePause":"(\\\d+)"', 1) as int) > 0
        group by 1
        )
        select 
            all_levels.*,
            Screen_Levels
        from all_levels left join screen_levels using (event_user)
        limit 10
        
        select * from main_day.all_events_30975_pq limit 10
        
                select 
                -- distinct regexp_extract(payload , '"PackId":"(\\\w+)"', 1) pack
                -- ,
                * 
                -- event_user,
                -- count (distinct cast(regexp_extract(payload , '"Level":"(\\\d+)"', 1) as int)) as Screen_Levels
        from 
            main_day.all_events_30975_pq 
        where 
            day >= unix_timestamp('2021-05-18') 
            and day <= unix_timestamp('2021-05-28')
            -- and event_type = 'purchase'
            -- and parameters like '%"Purchase."%'
            and parameters like '%"Purchase.Boosts"%'
            and payload like '%"AtScene":"0"%'
            -- and regexp_extract(payload , '"AtScene":"(\\\d+)"', 1)
            -- and parameters like '%"Purchase.BoostMagnifier"%'
            -- limit 10
            -- and (
            -- parameters like '%"ISPY.LevelComplete"%' 
            -- or parameters like '%"ISPY.LevelFailed"%'
            -- )
            -- and cast(regexp_extract(payload , '"MakeScreenshot":"(\\\d+)"', 1) as int) > 0
            -- and cast(regexp_extract(payload , '"UsePause":"(\\\d+)"', 1) as int) > 0
            


select * 
from main_day.all_events_30975_pq  
where event_type = 'event' 
and payload like '%Races%' 
-- and parameters like '%Racing.%' 
-- and parameters like '%misc.currency_given.coins.Racing%' 
-- and regexp_extract(parameters , '"item":"(\\\w+)"', 1) like '%"misc.boosters%' 
and day >= unix_timestamp('2021-08-01')  
and day <= unix_timestamp('2021-08-31') 
            -- and (
            -- parameters like '%"ISPY.LevelComplete"%' 
            -- or parameters like '%"ISPY.LevelFailed"%'
            -- )
limit 10



 select * from main_day.all_events_30975_pq limit 10
 
 with boosts as
 (
  select
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
--  limit 10
 )
select 
	atscene,
	packid,
	count(packid)
from boosts
group by 1,2
 

