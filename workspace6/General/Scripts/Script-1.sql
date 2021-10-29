select * from impala_main_day.all_events_30975_pq limit 10

select * from impala_main_hour.all_events_30975 limit 10

select 
	event_user,
	active::smallint,
	n,
	activ_7
from main_day.all_retention_30975_pq limit 10


select
                  event_user,
                  day,
                  'ios' as platform,
                  sum(offer_price) as rev
                 from impala_main_day.valid_iap_30975_pq vi 
                 where day between unix_timestamp('2021-05-31') and  unix_timestamp('2021-07-01')
                 group by 1,2,3
                 
                 
COPY temp.cohort_test
FROM '~/Downloads/cohorts.csv'
CSV
DELIMITER ',';


grant select on temp.cohort_test to group bi, group analytics;




grant select on temp.cohort_test to group analytics


grant update on temp.cohort_test to group analytics;


grant insert on temp.cohort_test to  group analytics;

grant delete on temp.cohort_test to  group analytics;


COPY temp.cohort_test
FROM 's3://mktg-redshift-exchange/cohorts.zip'
iam_role 'arn:aws:iam::449041058118:role/redshift-as-exchange-role';

select * from stl_load_errors order by starttime desc limit 1;

COPY temp.cohort_test
FROM 's3://mktg-redshift-exchange/cohorts.csv'
iam_role 'arn:aws:iam::449041058118:role/redshift-as-exchange-role'
DATEFORMAT AS 'YYYY-MM-DD'
delimiter ','
IGNOREHEADER 1;

DROP TABLE "temp".cohort_test;


CREATE TABLE IF NOT EXISTS "temp".cohort_test
(
	id BIGINT ENCODE az64
	,activity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,event_user VARCHAR(256)   ENCODE lzo
	,first_session_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payer BIGINT   ENCODE az64
	,rev VARCHAR(256)   ENCODE lzo
	,device_region VARCHAR(256)   ENCODE lzo
	,platform BIGINT   ENCODE az64
	,activity_week TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,first_session_week TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,activity_month TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,first_session_month TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,lt_period_day BIGINT   ENCODE az64
	,lt_period_week BIGINT   ENCODE az64
	,lt_period_month BIGINT   ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE "temp".cohort_test owner to afanasoglu_yu;


select 
	device_region as country,
	sum(
	case 
	when rev <> '' then convert(decimal(9,2), rev)
	else 0
	end 
	) as revenue
from temp.cohort_test 
group by 1
order by 2 desc

select cast(rev as decimal(4,1)) from temp.cohort_test limit 30;

select *
from main_day.device_region 
where alpha2 = 'KR'


    select
        --from_unixtime(sp.hour - 86400, 'yyyy-MM-dd') day_sp,
        --sp.hour - 86400 day_sp_ts,
        from_unixtime(sp.hour - 86400 ,  'yyyy-MM-dd') as activity_date,
        sp.event_user,
        -- sp.first_session,
        from_unixtime(cast(sp.first_session/1000 as int),  'yyyy-MM-dd') as first_session_date,
        sp.payer,
        rev.rev,
        su.device_region,
        --cast(cast(from_unixtime(sp.hour - 86400 ,  'yyyy-MM-dd') as timestamp) - 
        --cast(from_unixtime(cast(sp.first_session/1000 as int),  'yyyy-MM-dd') as timestamp) as BIGINT) lt_period_day,
        1 as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_30975_pq vi 
                 where day between unix_timestamp('{d_start}') and  unix_timestamp('{d_end}')
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
        left join main_day.seg_users_30975_pq su on sp.event_user = su.event_user
        --left join main_day.seg_users_30975_pq su using(event_user)
        --left join main_day.seg_iap_30975_pq su using(event_user)
    where 
        sp.hour - 86400 between unix_timestamp('{d_start}') and unix_timestamp('{d_end}')
        and sp.first_session >= unix_timestamp('{d_start}') * 1000
        and from_unixtime(su.hour - 86400 ,  'yyyy-MM-dd') = '{d_end}'
        and last_active between (sp.hour - 86400) * 1000 and sp.hour * 1000

    select 
        event_user,
        count(distinct device_region)
    from main_day.seg_users_30975_pq su
    where --su.event_user = '23022021-190402-UHdZO9E5'
    --and 
    from_unixtime(su.hour - 86400 ,  'yyyy-MM-dd') = '2021-06-30'
    group by 1
    
    
    select count(*) from temp.cohort_source
    
    select * from temp.cohort_source limit 10
    
    select 	
    	date_trunc('month', activity_date)::date as report_period,
    	device_region as country,
    	sum(rev)
    from temp.cohort_source
    group by 1,2
    
    
    select 
    	platform,
    	count(distinct event_user)
    from temp.cohort_source 
    where device_region is null
    group by 1
    order by 1
    
    
    
    
    drop table temp.cohort_source


create table temp.cohort_source as
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        su.device_region,
        1 as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_users_30975_pq su on sp.event_user = su.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (su.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
 --limit 10
  union all
select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        su.device_region,
        2 as platform
    from
        main_day.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_users_31756_pq su on sp.event_user = su.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (su.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
  union all
select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        su.device_region,
        3 as platform
    from
        main_day.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_users_31761_pq su on sp.event_user = su.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (su.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
select * from main_day.seg_players_30975_pq limit 10

select * from  main_day.seg_users_30975_pq limit 10

select * from  main_day.seg_users_31756_pq limit 10

select * from  main_day.valid_iap_30975_pq limit 10

drop table temp.cohort_source_install

create table temp.cohort_source_install as
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        si.device_region,
        1 as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
  union all
     select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        si.device_region,
        2 as platform
    from
        main_day.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        sp.payer,
        rev.rev,
        si.device_region,
        3 as platform
    from
        main_day.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev
                 from main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
-- limit 10
        
        
     select 
    	platform,
    	count(distinct event_user)
    from temp.cohort_source_install
    where device_region is null
    group by 1
    order by 1  
    
    select 
    	min(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'),
    	max(timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second')
    from main_day.seg_players_30975_pq sp	
    	
    	select count(*) from temp.cohort_source_install csi 
    	
    	select count(*) from temp.cohort_ios_2021 
    	
    	select * from temp.cohort_ios_2021 limit 10


    	drop table temp.cohort_ios_2021   
    	
create table temp.cohort_ios_2021 as  	
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        1 as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
    limit 10
union all

        
        
        select 
        	first_session_week,
        	count(distinct event_user)
        from temp.cohort_ios_2021
        group by 1
        order by 1
        
        
        with cohort_size_period as 
        (
        select 
        	first_session_week,
        	activity_week,
        	count(distinct event_user) as cohort_size_period
        from temp.cohort_ios_2021
        group by 1,2
        )
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size_period *1.0 / cohort_size as rr
        from cohort_size_period
        left join (
		select 
        	first_session_week,
        	count(distinct event_user) as cohort_size
        from temp.cohort_ios_2021
        group by 1
        order by 1
        ) as cohort_size using(first_session_week)
		order by 1,2
		
		
		
		-- cohorts calculation
		
		
		with cohort_size_period as 
        (
        select 
        	first_session_week,
        	activity_week,
        	count(distinct event_user) as cohort_size_period,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from temp.cohort_ios_2021
--        where payer = 1
        group by 1,2
        )
        ,
        paying_cohort as
        (
        select 
        	cohort_size_period.*,
        	cohort_size.cohort_size,
        	cohort_size.cohort_size_paying,
        	cohort_size_period *1.0 / cohort_size as rr,
        	cohort_size_period_paying *1.0 / cohort_size_paying as rr_paying,
        	revenue / cohort_size as arpu,
        	revenue / cohort_size_paying as arppu,
        	revenue / cohort_size_period_paying as arpppu,
        	orders * 1.0 / cohort_size as full_order_freq,
        	orders * 1.0 / cohort_size_paying as paying_order_freq,
        	cohort_size_paying *1.0 / cohort_size as paying_cohort_share
        from cohort_size_period
        left join (
		select 
        	first_session_week,
        	count(distinct event_user) as cohort_size,
        	count(distinct 
        	case when payer=1 then event_user
        	end
        	) as cohort_size_paying
        from temp.cohort_ios_2021
--        where payer = 1
        group by 1
        order by 1
        ) as cohort_size using(first_session_week)
		order by 1,2
       )
       select 
       *       
       from paying_cohort
       
       
               select 
        	first_session_week,
        	activity_week,
        	count(distinct event_user) as cohort_size_period,
--        	count(distinct 
--        	case when payer=1 then event_user
--        	end
--        	) as cohort_size_period_paying,
        	sum(rev) as revenue,
        	sum(orders) as orders
        from temp.cohort_ios_2021
        where payer = 1
        group by 1,2
        order by 1,2
        
        
        select 
        	first_session_week,
        	count(distinct event_user) as cohort_size
--        	count(distinct 
--        	case when payer=1 then event_user
--        	end
--        	) as cohort_size_paying
        from temp.cohort_ios_2021
        where payer = 1 and first_session_week = activity_week 
        group by 1
        order by 1
        
        
        
create table temp.cohort_MM_all_platforms_2021 as  	
    select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'ios' as platform
    from
        main_day.seg_players_30975_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_30975_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_30975_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
--    limit 10
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'gp' as platform
    from
        main_day.seg_players_31756_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_31756_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31756_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
union all
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
        
        
        
        select
        
        cohort_mm_all_platforms_2021
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'amz' as platform
    from
        main_day.seg_players_31761_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_31761_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_31761_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-01-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-01-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second' 
--   limit 30 
        
        select count(*) from temp.cohort_MM_all_platforms_2021
        
        select * from temp.cohort_MM_all_platforms_2021 limit 20 
        
        
        
        select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from temp.cohort_MM_all_platforms_2021 
        group by 1,2
        order by 1, 3 desc
        
        select count(*) from temp.cohort_hs_ios_6_2021
        
        select count(*) from temp.cohort_hs_gp_2021
        
        select count(*) from temp.cohort_hs_amz_2021
        
        
                select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from temp.cohort_hs_ios_6_2021
        group by 1,2
        order by 1, 3 desc
        
                        select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from temp.cohort_hs_gp_2021
        group by 1,2
        order by 1, 3 desc
        
                                select 
        	activity_month,
        	platform,
        	sum(rev) as revenue
        from temp.cohort_hs_amz_2021
        group by 1,2
        order by 1, 3 desc
        
        
        
        
        create table temp.cohort_hs_gp_3_2021 as 
            select
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date as activity_date,
        date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_month,
        date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date as activity_week,
        sp.event_user,
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as first_session_date,
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_month,
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date as first_session_week,
        (date_trunc('week', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 7 as lt_period_week,
        (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second')::date - 
        (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second')::date as lt_period_day,
        (date_trunc('month', (timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'))::date - 
        date_trunc('month', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date) / 30 as lt_period_month,
--        ndv(sp.event_user) over (
--        partition by date_trunc('week', (timestamp 'epoch' + (sp.first_session /1000)::float * interval '1 second'))::date
--        ) as week_cohort_size,
--        count(distinct event_user) over (partition by first_session_week, activity) as week_cohort_size_period,
        sp.payer,
        rev.rev,
        rev.orders,
        si.device_region,
        'gp' as platform
    from
        main_day.seg_players_4699_pq sp
        left join (
                select
                  event_user,
                  day,
                  sum(offer_price) as rev,
                  count(offer_price) as orders
                 from main_day.valid_iap_4699_pq vi 
                 where 
                  timestamp 'epoch' + day::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
                 group by 1,2
        ) as rev on (rev.event_user = sp.event_user and rev.day = (sp.hour - 86400))
left join main_day.seg_install_info_4699_pq si on sp.event_user = si.event_user
    where 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second'  between '2021-04-01' and '2021-06-30'
        and timestamp 'epoch' + (sp.first_session/1000)::float * interval '1 second'  >= '2021-04-01'
        and timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second'  = '2021-06-30'
        and timestamp 'epoch' + (sp.last_active/1000)::float * interval '1 second'  between 
        timestamp 'epoch' + (sp.hour - 86400)::float * interval '1 second' and timestamp 'epoch' + (sp.hour)::float * interval '1 second'
        
        
        
        
        
        