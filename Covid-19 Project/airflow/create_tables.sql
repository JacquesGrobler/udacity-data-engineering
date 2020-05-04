CREATE TABLE IF NOT EXISTS public.covid_19_cases_history (
	country varchar(256),
    country_code varchar(256), 
	reported_date date,
	confirmed_cases bigint,
	deaths bigint,
	recovered bigint,
    active_cases bigint,
    PRIMARY KEY (country, reported_date)
);

CREATE TABLE IF NOT EXISTS public.country_hospital_beds (
	country varchar(256) PRIMARY KEY,
	country_code varchar(256) NOT NULL,
	year_of_report varchar(256),
	hospital_beds_per_1000 integer,
    total_beds bigint
);

CREATE TABLE IF NOT EXISTS public.country_population (
	country varchar(256) PRIMARY KEY,
	country_code varchar(256) NOT NULL,
	population bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS public.country_tweets (
    country varchar(256) NOT NULL,
	country_code varchar(256),
    tweet_id bigint PRIMARY KEY,
    tweet varchar(10000),
	time timestamp
);

CREATE TABLE IF NOT EXISTS public.covid_19_cases_summary (
    rank_by_confirmed_cases integer NOT NULL,
	country varchar(256) PRIMARY KEY,
    country_code varchar(256),
	reported_date date NOT NULL,
	confirmed_cases bigint,
	new_confirmed_cases_from_previous_day bigint,
	deaths bigint,
    new_deaths_from_previous_day bigint,
    recovered bigint,
	new_recovered_from_previous_day bigint,
	active_cases bigint,
    new_active_cases_from_previous_day bigint,
    active_cases_per_1_million bigint,
    population bigint,
    approx_cases_needing_hospital_beds bigint,
    total_hospital_beds bigint,
    percentage_tweets_per_sample real
);