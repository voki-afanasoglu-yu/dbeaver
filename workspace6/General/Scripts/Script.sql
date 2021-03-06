
 		WITH tmp_variables AS (
		SELECT 
		   '2021-08-01'::DATE AS StartDate, 
		   '2021-08-08'::DATE AS FinishDate
		)
select
        *
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
        and event_user = '07022021-125746-BzH5AHFy'
        limit 10
        
        
        
 		WITH tmp_variables AS (
		SELECT 
		   '2021-08-01'::DATE AS StartDate, 
		   '2021-08-08'::DATE AS FinishDate
		)
select
        campaign,
        device_type,
        install_type,
        
        from vokigames.seg_install_info_30975_pq si 
        where timestamp 'epoch' + (si.hour - 86400)::float * interval '1 second' between (SELECT StartDate FROM tmp_variables) and (SELECT FinishDate FROM tmp_variables)
        and event_user = '07022021-125746-BzH5AHFy'
        limit 10
        
        
        
        
        with cohort as
        (
select 
        	platform,
			first_session_date::date,
        	activity_date::date - first_session_date::date as lt_period,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev)*.7 rev 
        from vokigames.mm_2020_05_2021_07
        where  first_session_date >='2021-07-20' -- and platform = 'ios' --and --first_session_date::date = activity_date::date
        group by 1,2,3
        order by 1,2,3
        )
        ,
        cohort_size as 
        (
select 
        	platform,
			first_session_date::date,
        	count(distinct event_user ) as cohort_size,
        	sum(rev)*.7 rev_cohort
        from vokigames.mm_2020_05_2021_07
        where  first_session_date >='2021-07-20'-- and platform = 'ios' --and --first_session_date::date = activity_date::date
        group by 1,2
        )
        select 
        cohort.*,
        cohort_size.cohort_size,
        cohort_size.rev_cohort,
        rev / cohort_size as arpu,
        sum((rev*1.0 / cohort_size)) over (partition by platform,first_session_date order by lt_period rows unbounded preceding) as RPI,
        cohort_size_period * 1.0 /cohort_size as rr
        from cohort left join cohort_size using (first_session_date,platform)
        order by platform, first_session_date, lt_period, platform
        
        
        
                with cohort as
        (
select 
        	platform,
			first_session_date::date,
        	activity_date::date - first_session_date::date as lt_period,
        	count(distinct event_user ) as cohort_size_period,
        	sum(rev)*.7 cor_rev 
        from vokigames.gs_2020_05_2021_07
--        where  first_session_date >='2021-07-20' -- and platform = 'ios' --and --first_session_date::date = activity_date::date
        group by 1,2,3
        order by 1,2,3
        )
        ,
        cohort_size as 
        (
select 
        	platform,
			first_session_date::date,
        	count(distinct event_user ) as cohort_size,
        	sum(rev)*.7 rev_cohort
        from vokigames.gs_2020_05_2021_07
--        where  first_session_date >='2021-07-20'-- and platform = 'ios' --and --first_session_date::date = activity_date::date
        group by 1,2
        )
        ,
        last_mile as 
        (
        select 
        cohort.*,
        cohort_size.cohort_size,
        cohort_size.rev_cohort,
        cor_rev *1.0 / cohort_size as arpu,
        sum((cor_rev * 1.0 / cohort_size)) over (partition by platform,first_session_date order by lt_period rows unbounded preceding) as RPI,
        cohort_size_period * 1.0 /cohort_size as rr
        from cohort left join cohort_size using (first_session_date,platform)
        order by platform, first_session_date, lt_period, platform
        )
        select 
        	platform,
        	lt_period,
        	avg(rpi) as avg_rpi
        from last_mile
        group by 1,2
        order by 1,2
        
        
        select * from vokigames.mm_main_metrics_2021 limit 10
        
        activity_date::text,
        project,
        dau,
        revenue,
        payers,
        payers_share,
        arppdau,
        arpdau,
        avg_dau_90,
        avg_rev_90,
        avg_arp_dau_90,
        avg_arpp_dau_90,
        avg_payers_share_90,
        avg_dau_30,
        avg_rev_30,
        avg_arp_dau_30,
        avg_arpp_dau_30,
        avg_payers_share_30

        
        
        