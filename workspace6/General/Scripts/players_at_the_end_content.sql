--https://app.asana.com/0/347104365046376/1199942794131742/f

select
    day,
    event_user,
    sum(offer_price * 0.7) rev
from
    main_day.valid_iap_{gp}_pq
where
    day between unix_timestamp('{d_start}') and unix_timestamp('{d_end}')
group by
    1,2
    
    
    
   WITH variables AS (
SELECT 
   '2021-05-06'::DATE AS d_start, 
   '2021-05-19'::DATE AS d_end
)
select 
--	day,
	sum(rev) rev
from
(
select
    timestamp 'epoch' + day * interval '1 second' as day,
--    event_user,
    sum(offer_price * 0.7) rev
from
    main_day.valid_iap_30975_pq
where
     timestamp 'epoch' + day * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
group by
    1--,2
union all
select
    timestamp 'epoch' + day * interval '1 second' as day,
--    event_user,
    sum(offer_price * 0.7) rev
from
    main_day.valid_iap_31756_pq
where
     timestamp 'epoch' + day * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
group by
    1--,2
)
--group by 1
--order by 1 

select *
from
    main_day.valid_iap_30975_pq
    limit 10
    
 -- ?????? ? ????? ????????
 
   WITH variables AS (
SELECT 
   '2021-05-06'::DATE AS d_start, 
   '2021-05-19'::DATE AS d_end
)
,
 vi as (
select
    day,
    event_user,
    sum(offer_price * 0.7) rev
from
    main_day.valid_iap_{gp}_pq
where
    day between unix_timestamp('{d_start}') and unix_timestamp('{d_end}')
group by
    1,2
),

players as (
select 
    hour-86400 as day,
    event_user,
    case
        -- when sp.level < 50 then '0_early_game'
        when su.payment_seg = 0 then '0_non_payers'
        when su.payment_seg <= {low_payment} then '1_low_payers'
        when su.payment_seg <= {normal_payment} then '2_normal_payers'
        when su.payment_seg <= {good_payment} then '3_good_payers'
        else '4_super_payers'
    end pay_seg
from 
    main_day.seg_users_{gp}_pq su
where
    hour-86400 between unix_timestamp('{d_start}') and unix_timestamp('{d_end}')
    and episode = '{episode}'
    and episode_day = {episode_day}
    and last_quest_completed = 1
)

select
    '{d_start}' as "?????? ???????",
    '{d_end}' as "????? ???????",
    pay_seg as "??????? ??????",
    'gp' as "?????????",
    count(distinct players.event_user) "????????? ????",
    sum(ifnull(vi.rev,0)) "??????? ????????? ????"
from
    players left join vi
        on
            players.event_user = vi.event_user
            and players.day = vi.day
group by
    1,2,3,4
order by pay_seg


WITH variables AS (
SELECT 
   '2021-09-16'::DATE AS d_start, 
   '2021-09-29'::DATE AS d_end,
   'CH_Bedroom'::text as episode,
   	'4'::text as episode_day
)
select 
*
from 
    main_day.seg_users_30975_pq su
where
    timestamp 'epoch' + (hour-86400) * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
--    and episode = (SELECT episode FROM variables)
--    and episode_day = (SELECT episode_day FROM variables)
    and last_quest_completed = 1
    limit 10
    
    
    
    WITH variables AS (
SELECT 
   '2021-09-16'::DATE AS d_start, 
   '2021-09-29'::DATE AS d_end,
   'CH_Bedroom'::text as episode,
   	'4'::text as episode_day
)
select 
json_extract_path_text(payload, 'LastQuestCompleted'),
json_extract_path_text(payload, 'EndLevel')
,
*
from 
    main_day.all_events_30975_pq 
where
    timestamp 'epoch' + day * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
--    and episode = (SELECT episode FROM variables)
--    and episode_day = (SELECT episode_day FROM variables)
    and event_type = 'event'
    and (parameters like '%"ISPY.LevelFailed"%' or parameters like '%"ISPY.LevelComplete"%')
    and payload like '%"CH_Bedroom"%'
    and json_extract_path_text(payload, 'EndLevel') = '1'
    and json_extract_path_text(payload, 'LastQuestCompleted') = '1'
    and json_extract_path_text(payload, 'Day') = '4'
limit 10
    
    
    


select 
*
from 
    main_day.attempts_30975_pq ap 
--where
--    timestamp 'epoch' + (hour-86400) * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
 limit 10
 
 
 select 
*
from 
    main_day.all_events_30975_pq aep 
--where
--    timestamp 'epoch' + (hour-86400) * interval '1 second' between (SELECT d_start FROM variables) and (SELECT d_end FROM variables)
 limit 10
 
 
    