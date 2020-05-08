Select Id,
	Phoenix_People_Id__pc,
	LastModifiedDate,
	replicated_from_phoenix__pc,
	phoenix_replication_date_check__pc,
	Phoenix_People_Created_At__pc
	From Account
Where PersonEmail in ('testing@getsmarter.co.za', 'testcrm@getsmarter.co.za', 'testcrm2@getsmarter.co.za', 'test@test.test', 'testing@getsmarter.com', 'test@test.com', 'testing2@getsmarter.co.za', 'testtesting@getsmarter.co.za')

CREATE TABLE IF NOT EXISTS public.covid_19_cases_history (
country varchar(256),
country_code varchar(256), 
reported_date date,
confirmed_cases integer,
deaths integer,
recovered integer,
active_cases integer,
PRIMARY KEY (country, reported_date)
);

CREATE TABLE IF NOT EXISTS public.country_hospital_beds (
country varchar(256) PRIMARY KEY,
country_code varchar(256) NOT NULL,
year_of_report varchar(256),
hospital_beds_per_1000 integer,
total_beds integer
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
confirmed_cases integer,
new_confirmed_cases_from_previous_day integer,
deaths integer,
new_deaths_from_previous_day integer,
recovered integer,
new_recovered_from_previous_day integer,
active_cases integer,
new_active_cases_from_previous_day integer,
active_cases_per_1_million integer,
population integer,
approx_cases_needing_hospital_beds integer,
total_hospital_beds integer,
percentage_tweets_per_sample real
);
