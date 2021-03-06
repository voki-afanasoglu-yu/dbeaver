select 
	distinct name
from main_day.attempts_4698_pq
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-13' and '2021-10-17'
limit 10

with users as
(

create table vokigames.temp_hs_levels_speed as

create table vokigames.temp_hs_levels_speed_attempts as

insert into vokigames.temp_hs_levels_speed_attempts
select 
	(timestamp 'epoch' + day * interval '1 second')::date as report_period,
	att.event_user,
	payer,
	case
        when sp.level < 50 then '0_early_game'
        when (su.payment_seg = 0 and su.skill_seg <= 0.411) then '0_non_payers_skill_1'
        when (su.payment_seg = 0 and su.skill_seg <= 0.663) then '0_non_payers_skill_2'
        when (su.payment_seg = 0 and su.skill_seg > 0.663) then '0_non_payers_skill_3'
        when (su.payment_seg <= 1.12 and su.skill_seg <= 0.584) then '1_low_payers_skill_1'
        when (su.payment_seg <= 1.12 and su.skill_seg <= 0.829) then '1_low_payers_skill_2'
        when (su.payment_seg <= 1.12 and su.skill_seg > 0.829) then '1_low_payers_skill_3'
        when su.payment_seg <= 3.575 then '2_normal_payers'
        when su.payment_seg <= 7.912 then '3_good_payers'
        when su.payment_seg > 7.912 then '4_super_payers'
    end as sp_seg,
	count(name) as wins,
	avg(attempt) as avg_attempt,
	median(attempt) as med_attempt,
	sum(attempt) as all_attempt,
	nvl(sum(
	case
	when attempt = 1 then attempt
	end
	),0) as first_attempt_win
from impala_main_day.attempts_4698_pq att
left join
impala_main_day.seg_users_4698_pq su on su.event_user = att.event_user
left join
impala_main_day.seg_players_4698_pq sp on sp.event_user = su.event_user
where 
	timestamp 'epoch' + day * interval '1 second' between '2021-09-18' and '2021-10-19'
--	timestamp 'epoch' + day * interval '1 second' between '2021-09-16' and '2021-09-17'
	and name = 'Level.LevelComplete'
	and timestamp 'epoch' + (su.hour-86400) * interval '1 second' = '2021-10-19'
	and timestamp 'epoch' + (sp.hour-86400) * interval '1 second' = '2021-10-19'
group by 1,2,3,4
order by 1
--limit 10

-- ?????? ???


-- ??? ??????
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from vokigames.temp_hs_levels_speed_attempts

-- ?????? - ??????????? ?????????????
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- ?????? - ???????? ????????????

select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????

select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1


-- ?????? ??????


-- ??? ??????

with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3 
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	payer,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
--where report_period between '2021-08-' and '2021-10-19'
group by 1,2,3
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- ?????? 5 ??????


-- ??? ??????

with weeks as
(
select 
	event_user,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1 
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	event_user,
	payer,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(wins) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- ?????? ? 1 ????

-- ?????? ? 1 ???? ???

select * from vokigames.temp_hs_levels_speed_attempts limit 10

-- ??? ??????
select
  percentile_cont(0.25) within group (order by first_attempt_win asc) as p_25,
  percentile_cont(0.50) within group (order by first_attempt_win asc) as p_50,
  percentile_cont(0.65) within group (order by first_attempt_win asc) as p_65,
  percentile_cont(0.75) within group (order by first_attempt_win asc) as p_75,
  percentile_cont(0.85) within group (order by first_attempt_win asc) as p_85,
  percentile_cont(0.90) within group (order by first_attempt_win asc) as p_90,
  percentile_cont(0.95) within group (order by first_attempt_win asc) as p_95,
  percentile_cont(0.98) within group (order by first_attempt_win asc) as p_98,
  avg(first_attempt_win) as mean
from vokigames.temp_hs_levels_speed_attempts

-- ?????? - ??????????? ?????????????
select
	payer,
  percentile_cont(0.25) within group (order by first_attempt_win asc) as p_25,
  percentile_cont(0.50) within group (order by first_attempt_win asc) as p_50,
  percentile_cont(0.65) within group (order by first_attempt_win asc) as p_65,
  percentile_cont(0.75) within group (order by first_attempt_win asc) as p_75,
  percentile_cont(0.85) within group (order by first_attempt_win asc) as p_85,
  percentile_cont(0.90) within group (order by first_attempt_win asc) as p_90,
  percentile_cont(0.95) within group (order by first_attempt_win asc) as p_95,
  percentile_cont(0.98) within group (order by first_attempt_win asc) as p_98,
  avg(first_attempt_win) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- ?????? - ???????? ????????????

select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by first_attempt_win asc) as p_25,
  percentile_cont(0.50) within group (order by first_attempt_win asc) as p_50,
  percentile_cont(0.65) within group (order by first_attempt_win asc) as p_65,
  percentile_cont(0.75) within group (order by first_attempt_win asc) as p_75,
  percentile_cont(0.85) within group (order by first_attempt_win asc) as p_85,
  percentile_cont(0.90) within group (order by first_attempt_win asc) as p_90,
  percentile_cont(0.95) within group (order by first_attempt_win asc) as p_95,
  percentile_cont(0.98) within group (order by first_attempt_win asc) as p_98,
  avg(first_attempt_win) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????

select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by first_attempt_win asc) as p_25,
  percentile_cont(0.50) within group (order by first_attempt_win asc) as p_50,
  percentile_cont(0.65) within group (order by first_attempt_win asc) as p_65,
  percentile_cont(0.75) within group (order by first_attempt_win asc) as p_75,
  percentile_cont(0.85) within group (order by first_attempt_win asc) as p_85,
  percentile_cont(0.90) within group (order by first_attempt_win asc) as p_90,
  percentile_cont(0.95) within group (order by first_attempt_win asc) as p_95,
  percentile_cont(0.98) within group (order by first_attempt_win asc) as p_98,
  avg(first_attempt_win) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1


-- ?????? ? 1 ???? ??????


-- ??? ??????

with weas
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	payer,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
--where report_period between '2021-08-' and '2021-10-19'
group by 1,2,3
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- ?????? ? 1 ???? 5 ??????


-- ??? ??????

with weeks as
(
select 
	event_user,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1 
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	event_user,
	payer,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(first_attempt_win) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1



-- ??????? ?? ?????

-- ??????? ?? ????? ???

select * from vokigames.temp_hs_levels_speed_attempts limit 10

-- ??? ??????
select
  percentile_cont(0.25) within group (order by all_attempt asc) as p_25,
  percentile_cont(0.50) within group (order by all_attempt asc) as p_50,
  percentile_cont(0.65) within group (order by all_attempt asc) as p_65,
  percentile_cont(0.75) within group (order by all_attempt asc) as p_75,
  percentile_cont(0.85) within group (order by all_attempt asc) as p_85,
  percentile_cont(0.90) within group (order by all_attempt asc) as p_90,
  percentile_cont(0.95) within group (order by all_attempt asc) as p_95,
  percentile_cont(0.98) within group (order by all_attempt asc) as p_98,
  avg(all_attempt) as mean
from vokigames.temp_hs_levels_speed_attempts


-- ?????? - ??????????? ?????????????
select
	payer,
  percentile_cont(0.25) within group (order by all_attempt asc) as p_25,
  percentile_cont(0.50) within group (order by all_attempt asc) as p_50,
  percentile_cont(0.65) within group (order by all_attempt asc) as p_65,
  percentile_cont(0.75) within group (order by all_attempt asc) as p_75,
  percentile_cont(0.85) within group (order by all_attempt asc) as p_85,
  percentile_cont(0.90) within group (order by all_attempt asc) as p_90,
  percentile_cont(0.95) within group (order by all_attempt asc) as p_95,
  percentile_cont(0.98) within group (order by all_attempt asc) as p_98,
  avg(all_attempt) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- ?????? - ???????? ????????????

select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by all_attempt asc) as p_25,
  percentile_cont(0.50) within group (order by all_attempt asc) as p_50,
  percentile_cont(0.65) within group (order by all_attempt asc) as p_65,
  percentile_cont(0.75) within group (order by all_attempt asc) as p_75,
  percentile_cont(0.85) within group (order by all_attempt asc) as p_85,
  percentile_cont(0.90) within group (order by all_attempt asc) as p_90,
  percentile_cont(0.95) within group (order by all_attempt asc) as p_95,
  percentile_cont(0.98) within group (order by all_attempt asc) as p_98,
  avg(all_attempt) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????

select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by all_attempt asc) as p_25,
  percentile_cont(0.50) within group (order by all_attempt asc) as p_50,
  percentile_cont(0.65) within group (order by all_attempt asc) as p_65,
  percentile_cont(0.75) within group (order by all_attempt asc) as p_75,
  percentile_cont(0.85) within group (order by all_attempt asc) as p_85,
  percentile_cont(0.90) within group (order by all_attempt asc) as p_90,
  percentile_cont(0.95) within group (order by all_attempt asc) as p_95,
  percentile_cont(0.98) within group (order by all_attempt asc) as p_98,
  avg(all_attempt) as mean
from vokigames.temp_hs_levels_speed_attempts
group by 1
order by 1


-- ?????? ? 1 ???? ??????


-- ??? ??????

with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	payer,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
group by 1,2,3
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	date_trunc('week', report_period) as report_period, 
	event_user,
	sp_seg,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
--where report_period between '2021-08-' and '2021-10-19'
group by 1,2,3
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- ?????? ? 1 ???? 5 ??????


-- ??? ??????

with weeks as
(
select 
	event_user,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1 
order by 1
)
select
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks

-- ?????? - ??????????? ?????????????

with weeks as
(
select 
	event_user,
	payer,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	payer,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1


-- -- ?????? - ???????? ????????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%0_non_payers%' then '0_non_payers'
	when sp_seg like '%1_low_payers%' then '1_low_payers'
	else sp_seg
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1

-- -- -- ?????? - ???????? ???????????


with weeks as
(
select 
	event_user,
	sp_seg,
	sum(all_attempt) as wins
from vokigames.temp_hs_levels_speed_attempts
where report_period between '2021-09-15' and '2021-10-19'
group by 1,2
)
select
	case
	when sp_seg like '%skill_1%' then 'high_skill'
	when sp_seg like '%skill_2%' then 'mid_skill'
	when sp_seg like '%skill_3%' then 'low_skill'
	end 
	as sp_seg,
  percentile_cont(0.25) within group (order by wins asc) as p_25,
  percentile_cont(0.50) within group (order by wins asc) as p_50,
  percentile_cont(0.65) within group (order by wins asc) as p_65,
  percentile_cont(0.75) within group (order by wins asc) as p_75,
  percentile_cont(0.85) within group (order by wins asc) as p_85,
  percentile_cont(0.90) within group (order by wins asc) as p_90,
  percentile_cont(0.95) within group (order by wins asc) as p_95,
  percentile_cont(0.98) within group (order by wins asc) as p_98,
  avg(wins) as mean
from weeks
group by 1
order by 1






select distinct sp_seg  from vokigames.temp_hs_levels_speed_attempts



select *
from vokigames.temp_hs_levels_speed_attempts
limit 10


select *
from vokigames.temp_hs_levels_speed
limit 10

select distinct(report_period)
from vokigames.temp_hs_levels_speed_attempts
order by 1

select 
	report_period,
	avg(wins)
from vokigames.temp_hs_levels_speed
group by 1
order by 1

select 
--	report_period,
	avg(wins),
	median(wins),
	max(wins)
from vokigames.temp_hs_levels_speed
--group by 1
--order by 1

--limit 10


select * from main_day.seg_players_4698_pq limit 10

select max(timestamp 'epoch' + (su.hour-86400) * interval '1 second') from impala_main_day.seg_users_4698_pq su
where timestamp 'epoch' + (su.hour-86400) * interval '1 second' > '2021-09-01'
--limit 10


select 
	*
from main_day.attempts_4698_pq
where name = 'Level.LevelComplete'
limit 10



    case
 		when sp.level < 50 then '0_early_game'
        when su.payment_seg = 0 then '0_non_payers'
        when su.payment_seg <= {low_payment} then '1_low_payers'
        when su.payment_seg <= {normal_payment} then '2_normal_payers'
        when su.payment_seg <= {good_payment} then '3_good_payers'
        else '4_super_payers'
    end pay_seg,
from 


case
        when sp.level < 50 then '0_early_game'
        when (su.payment_seg = 0 and su.skill_seg <= 0.411) then '0_non_payers_skill_1'
        when (su.payment_seg = 0 and su.skill_seg <= 0.663) then '0_non_payers_skill_2'
        when (su.payment_seg = 0 and su.skill_seg > 0.663) then '0_non_payers_skill_3'
        when (su.payment_seg <= 0.991 and su.skill_seg <= 0.584) then '1_low_payers_skill_1'
        when (su.payment_seg <= 0.991 and su.skill_seg <= 0.829) then '1_low_payers_skill_2'
        when (su.payment_seg <= 0.991 and su.skill_seg > 0.829) then '1_low_payers_skill_3'
        when su.payment_seg <= 3.075 then '2_normal_payers'
        when su.payment_seg <= 6.912 then '3_good_payers'
        when su.payment_seg > 6.912 then '4_super_payers'
    end as sp_seg,
    count(sp.event_user) as all_users
    from
        main_day.seg_players_{platform}_pq sp
            join
        main_day.seg_users_{platform}_pq su using(event_user)
    main_day.seg_users_{platform}_pq su
    
