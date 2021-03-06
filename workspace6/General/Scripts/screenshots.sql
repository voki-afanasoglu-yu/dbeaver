--https://app.asana.com/0/347104365046376/1200790034571659/f

select 
	*
from vokigames.attempts_30975_pq 
limit 10


select
	*
from vokigames.all_events_30975_pq
limit 10

select 
    payload,
    event_time,
    event_user
from 
    main_day.all_events_30975_pq
where 
    timestamp 'epoch' + (day)::float * interval '1 second' between '2021-07-01' and '2021-07-31'
    and event_type = 'event'
    and parameters like '%ISPY.UsePause%'
    
    
    select 
--	payload,
    event_time,
    event_user,
    json_extract_path_text(payload , 'Payers') as Payers,
    json_extract_path_text(payload , 'MakeScreenshot') as MakeScreenshot,
    json_extract_path_text(payload , 'Difficulty') as Difficulty,
    json_extract_path_text(payload , 'Mode') as Mode,
    json_extract_path_text(payload , 'Chapter') as Chapter,
    json_extract_path_text(payload , 'PauseCount') as PauseCount,
    json_extract_path_text(payload , 'Name') as Name,
    json_extract_path_text(payload , 'Attempt') as Attempt,
    json_extract_path_text(payload , 'Level') as Level,
    json_extract_path_text(payload , 'TimeLeftPause') as TimeLeftPause     
--     JSON_PARSE(payload)
from 
    main_day.all_events_30975_pq
where 
    timestamp 'epoch' + (day)::float * interval '1 second' between '2021-07-31' and '2021-07-31'
    and event_type = 'event'
    and json_extract_path_text(parameters, 'name') = 'ISPY.UsePause'
limit 10
    
    
    
    select count(*)
from 
    main_day.all_events_30975_pq
where 
    timestamp 'epoch' + (day)::float * interval '1 second' between '2021-05-01' and '2021-07-31'
    and event_type = 'event'
    and json_extract_path_text(parameters, 'name') = 'ISPY.UsePause'
    and json_extract_path_text(payload , 'MakeScreenshot')::int > 0
    
    with source as
    (
    
    create table vokigames.mm_screenshots_2021_05_08_ios_ispy as
    
    
    select
    event_time,
    event_user,
    json_extract_path_text(payload , 'PayerType') as PayerType,
    json_extract_path_text(payload , 'Payers') as Payers,
    json_extract_path_text(payload , 'MakeScreenshot') as MakeScreenshot,
    json_extract_path_text(payload , 'Difficulty') as Difficulty,
    json_extract_path_text(payload , 'Mode') as Mode,
    json_extract_path_text(payload , 'Chapter') as Chapter,
    json_extract_path_text(payload , 'PauseCount') as PauseCount,
    json_extract_path_text(payload , 'Name') as Name,
    json_extract_path_text(payload , 'Attempt') as Attempt,
    json_extract_path_text(payload , 'Level') as Level,
--    json_extract_path_text(payload , 'TimeLeftPause') as TimeLeftPause,
    json_extract_path_text(parameters, 'name') as event_name
from 
    main_day.all_events_30975_pq
where 
    timestamp 'epoch' + (day)::float * interval '1 second' between '2021-05-18' and '2021-05-19'
    and event_type = 'event'
    and json_extract_path_text(parameters, 'name') like '%ISPY%'
--    and json_extract_path_text(payload , 'MakeScreenshot')::int > 0
    
    select * from vokigames.mm_screenshots_2021_05_08_ios limit 10
    
    
    insert into vokigames.mm_screenshots_2021_05_08_ios
    select
    event_time,
    event_user,
    json_extract_path_text(payload , 'PayerType') as PayerType,
    json_extract_path_text(payload , 'Payers') as Payers,
    json_extract_path_text(payload , 'MakeScreenshot') as MakeScreenshot,
    json_extract_path_text(payload , 'Difficulty') as Difficulty,
    json_extract_path_text(payload , 'Mode') as Mode,
    json_extract_path_text(payload , 'Chapter') as Chapter,
    json_extract_path_text(payload , 'PauseCount') as PauseCount,
    json_extract_path_text(payload , 'Name') as Name,
    json_extract_path_text(payload , 'Attempt') as Attempt,
    json_extract_path_text(payload , 'Level') as Level,
--    json_extract_path_text(payload , 'TimeLeftPause') as TimeLeftPause,
    json_extract_path_text(parameters, 'name') as event_name
from 
    main_day.all_events_30975_pq
where 
    timestamp 'epoch' + (day)::float * interval '1 second' between '2021-07-19' and '2021-08-18'
    and event_type = 'event'
    and json_extract_path_text(parameters, 'name') = 'ISPY.UsePause'
--    and json_extract_path_text(payload , 'MakeScreenshot')::int > 0
    
    
    )
    select 
            PayerType,
--        count(distinct event_user) as all_users,
        count(distinct
            case
            when MakeScreenshot > 0 and pausecount > 0 then event_user
            end
            ) as screenshoter_pauser
    from source
    group by 1
    
    
    select distinct level from vokigames.mm_screenshots_2021_05_08_ios
    order by 1
    
    
    select
    	level::int,
    	count(
    	distinct 
    	case 
    	when makescreenshot::int > 0 then event_user
    	end
    	) as screen_user,
    	count (distinct event_user) all_users
    from vokigames.mm_screenshots_2021_05_08_ios
--    where makescreenshot::int > 0
    group by 1
    order by 1
    
        select
    	payertype,
    	count(
    	distinct 
    	case 
    	when makescreenshot::int > 0 then event_user
    	end
    	) as screen_user,
    	count (distinct event_user) all_users
    from vokigames.mm_screenshots_2021_05_08_ios
--    where makescreenshot::int > 0
    group by 1
    order by 1
    
    
    
    
    update vokigames.mm_screenshots_2021_05_08_ios
set makescreenshot=0
where makescreenshot = ' '

    update vokigames.mm_screenshots_2021_05_08_ios
set level=0
where level = ' '

with ttt as
(
select 
	event_user,
	json_extract_path_text(payload , 'Name') as name
--	timestamp 'epoch' + (event_time/1000)::float * interval '1 second',
--	json_extract_path_text(payload , 'MakeScreenshot'),
--	json_extract_path_text(payload , 'UsePause'),
--	*
from main_day.all_events_30975_pq
where (json_extract_path_text(parameters, 'name') = 'ISPY.LevelComplete' 
or json_extract_path_text(parameters, 'name') = 'ISPY.LevelFailed')
and json_extract_path_text(payload , 'MakeScreenshot') not in ('', '0')
and json_extract_path_text(payload , 'UsePause') not in ('', '0')
and timestamp 'epoch' + (event_time/1000)::float * interval '1 second' between '2021-05-18' and '2021-05-19'
--limit 10
)
	select
   		name,
    	count(event_user) as screeners
   from ttt
   group by 1

   
   -- ???????
   
   with ttt as
(
select 
	event_user,
--	json_extract_path_text(payload , 'Name') as name
--	timestamp 'epoch' + (event_time/1000)::float * interval '1 second',
--	json_extract_path_text(payload , 'MakeScreenshot'),
--	json_extract_path_text(payload , 'UsePause'),
--	*
from main_day.all_events_30975_pq
left join 
(
	select --* 
		event_user,
		min(max_level),
		max(max_level),
		max(max_level) - min(max_level)
	from main_day.seg_users_30975_pq
	where timestamp 'epoch' + (hour - 86400 )::float * interval '1 second' between '2021-05-18' and '2021-08-18'
	group by 1
) as levels using (event_user)
where (json_extract_path_text(parameters, 'name') = 'ISPY.LevelComplete' or json_extract_path_text(parameters, 'name') = 'ISPY.LevelFailed')
and json_extract_path_text(payload , 'MakeScreenshot') not in ('', '0')
and json_extract_path_text(payload , 'UsePause') not in ('', '0')
and timestamp 'epoch' + (event_time/1000)::float * interval '1 second' between '2021-05-18' and '2021-05-19'
--limit 10
)
	select
   		name,
    	count(event_user) as screeners
   from ttt
   group by 1
   
	select --* 
		event_user,
		min(max_level),
		max(max_level),
		max(max_level) - min(max_level)
	from main_day.seg_users_30975_pq
	where timestamp 'epoch' + (hour - 86400 )::float * interval '1 second' between '2021-05-18' and '2021-08-18'
	group by 1
--	having max(max_level) - min(max_level) > 1
--	limit 100
   
	-- ??????? ??????? ?? ?????? ???????????? ?????? ? ?????
	
	select * from main_day.attempts_30975_pq  limit 10
	
	
	
	
	
    	
    