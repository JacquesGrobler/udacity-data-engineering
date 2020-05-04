class SqlQueries:
    cases_daily_table_insert = ("""
        select distinct 
        case when (b.alpha_3_code is null or b.alpha_3_code = '')
        then a.country else b.country
        end as country,
        b.alpha_3_code as country_code,
        cast(a.date_of_record as date) as reported_date,
        sum(a.confirmed) as confirmed_cases,
        sum(a.deaths) as deaths,
        sum(a.recovered) as recovered,
        sum(a.confirmed) - (sum(deaths) + sum(recovered)) as active_cases
        from public.covid_19_cases_stage as a
        left join public.country_iso_stage as b on (a.country = b.country or a.country = b.alpha_2_code or a.country = b.alpha_3_code)
        where a.country is not null
        and a.date_of_record is not null
        group by 1,2,3;
    """)

    hospital_beds_table_insert = ("""
        select 
        case when (c.alpha_3_code is null or c.alpha_3_code = '')
        then a.country else c.country
        end as country,
        a.country_code,
        a.year_of_report,
        a.hospital_beds_per_1000,
        a.total_beds
        from
        (select distinct
        p.country,
        h.country_code,
        h.year as year_of_report,
        h.hospital_beds_per_1000,
        (h.hospital_beds_per_1000*p.population)/1000 as total_beds                                     
        from public.hospital_beds_stage as h
        left join public.population_by_country_stage as p on h.country_code = p.alpha_3_code
        where p.country is not null
        and h.country_code is not null
        and h.hospital_beds_per_1000 is not null) as a
        left join public.country_iso_stage as c on a.country_code = c.alpha_3_code
    """)

    population_table_insert = ("""
        select distinct
        case when (c.alpha_3_code is null or c.alpha_3_code = '')
        then p.country else c.country
        end as country,
        p.alpha_3_code as country_code,
        p.population
        from public.population_by_country_stage as p
        left join public.country_iso_stage as c on p.alpha_3_code = c.alpha_3_code
        where p.population is not null
        and p.alpha_3_code is not null
        and p.country is not null;
    """)
    
    covid_19_cases_summary = ("""
        select distinct
        rank() over(order by a.confirmed_cases desc) as rank_by_confirmed_cases,
        a.country,
        a.country_code,
        a.reported_date,
        a.confirmed_cases,
        (a.confirmed_cases - b.confirmed_cases) as new_confirmed_cases_from_previous_day,
        a.deaths,
        (a.deaths - b.deaths) as new_deaths_from_previous_day,
        a.recovered,
        (a.recovered - b.recovered) as new_recovered_from_previous_day,
        a.active_cases,
        (a.active_cases - b.active_cases) as new_active_cases_from_previous_day,
        (a.active_cases*1000000)/c.population as active_cases_per_1_million,
        c.population,
        a.active_cases*0.05 as approx_cases_needing_hospital_beds,
        d.total_beds as total_hospital_beds,
        e.percentage_of_sample as percentage_tweets_per_sample
        from (
        select 
        rank() over(partition by country order by reported_date desc) as rank
        , * 
        from public.covid_19_cases_history) as a
        left join public.covid_19_cases_history as b on (a.country = b.country and (a.reported_date - interval '1 day') = b.reported_date)
        left join public.country_population as c on (a.country = c.country)
        left join public.country_hospital_beds as d on (a.country = d.country)
        left join 
        (select 
          t.country,
          ((t.tweet_count*100)/(sum(t.tweet_count) over ()))::real as percentage_of_sample
          from
          (select 
          country,
          count(tweet) as tweet_count
          from public.country_tweets
          group by 1) as t) as e on (a.country = e.country)
        where a.rank = 1; 
    """)
    
    tweets_insert = ("""
        select distinct 
        case when (c.alpha_3_code is null or c.alpha_3_code = '')
        then a.country else c.country
        end as country,
        c.alpha_3_code as country_code,
        a.tweet_id,
        a.tweet,
        a.time
        from 
        (select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (SPLIT_PART(t.user_location, ', ', 1) = b.country)
        where b.country is not null

        union all 
        
        select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (SPLIT_PART(t.user_location, ', ', 2) = b.country)
        where b.country is not null

        union all 

        select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (SPLIT_PART(t.user_location, ', ', 2) = b.iso3)
        where b.country is not null

        union all

        select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (SPLIT_PART(t.user_location, ', ', 1) = b.City)
        where b.country is not null
        
        union all

        select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (SPLIT_PART(t.user_location, ', ', 2) = b.City)
        where b.country is not null

        union all

        select
        b.country,
        b.iso3,
        t.tweet_id,
        t.tweet,
        t.time
        from public.tweets_stage as t
        LEFT join public.cities_stage as b on (t.country like '%'+b.Country+'%')
        where b.country is not null) as a
        left join public.country_iso_stage as c on (a.country = c.country or a.iso3 = c.alpha_3_code);
    """
    )
    
    create_country_iso = ("""
        CREATE TABLE public.country_iso_stage (
        index integer,
        country varchar(256),
        alpha_2_code varchar(256),
        alpha_3_code varchar(256),
        numeric_code integer
        );
    """)
                          
    create_country_geographic_disbtribution = ("""
        CREATE TABLE IF NOT EXISTS public.covid_19_geographic_disbtribution_stage (
        reported_date varchar(256),
        day integer,
        month integer,
        year integer,
        cases integer,
        deaths integer,
        country varchar(256),
        alpha_2_code varchar(256),
        alpha_3_code varchar(256),   
        population bigint
        );
    """)
                          
    create_hospital_beds = ("""
        CREATE TABLE IF NOT EXISTS public.hospital_beds_stage (
        country_code varchar(256),
        inticator varchar(256),
        subject varchar(256),
        measure varchar(256),
        freaquency varchar(256),
        year integer,
        hospital_beds_per_1000 numeric(18,0)
        );
    """)
                          
    create_population_by_country = ("""
        CREATE TABLE IF NOT EXISTS public.population_by_country_stage (
        index integer,
        country varchar(256),
        alpha_3_code varchar(256),
        indicator_name varchar(256),
        indicator_code varchar(256),
        population numeric(18,0)
        );
    """)
                          
    create_covid_19_cases = ("""
        CREATE TABLE IF NOT EXISTS public.covid_19_cases_stage (
        state varchar(256),
        country varchar(256),
        lat varchar(256),
        long varchar(256),
        date_of_record varchar(256),
        confirmed integer,
        deaths integer,
        recovered integer
    );
    """)
    
    create_tweets = ("""
        CREATE TABLE IF NOT EXISTS public.tweets_stage (
        tweet varchar(10000),
        tweet_id bigint,
        time timestamp,
        favorited int,
        retweeted int,
        ss_favourited int,
        is_retweeted int,
        is_Retweet int,
        retweet_from varchar(1024),
        country varchar(1024),
        user_name varchar(1024),
        user_id bigint,
        user_description varchar(1024),
        user_creation_time timestamp,
        user_language varchar(1024),
        user_location varchar(1024),
        user_time_zone varchar(1024),
        user_statuses int,
        user_followers int,
        user_friends int,
        user_favourites int
    );
    """)
    
    create_cities = ("""
        CREATE TABLE IF NOT EXISTS public.cities_stage (
        city varchar(256),
        city_ascii varchar(256),
        lat varchar(256),
        lng varchar(256),
        country varchar(256),
        iso2 varchar(256),
        iso3 varchar(256),
        admin_name varchar(256),
        capital varchar(256),
        population numeric,
        id bigint
    );
    """)
    
    delete_country_iso = "DROP TABLE IF EXISTS public.country_iso_stage;"
    delete_hospital_beds = "DROP TABLE IF EXISTS public.hospital_beds_stage;"
    delete_population_by_country = "DROP TABLE IF EXISTS public.population_by_country_stage;"  
    delete_covid_19_cases = "DROP TABLE IF EXISTS public.covid_19_cases_stage;" 
    delete_tweets = "DROP TABLE IF EXISTS public.tweets_stage;" 
    delete_cities = "DROP TABLE IF EXISTS public.cities_stage;" 
  
    create_table_queries = [create_country_iso, create_hospital_beds, create_population_by_country, create_covid_19_cases, create_tweets, create_cities]
    delete_table_queries = [delete_country_iso, delete_hospital_beds, delete_population_by_country, delete_covid_19_cases, delete_tweets, delete_cities]
                          
    