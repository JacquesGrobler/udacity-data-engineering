CREATE TABLE public.country_iso (
	index integer NOT NULL,
	country varchar(256) NOT NULL,
	alpha_2_code varchar(256) NOT NULL,
	alpha_3_code varchar(256)  NOT NULL,
	numeric_code integer NOT NULL
);

CREATE TABLE public.covid_19_geographic_disbtribution (
	reported_date varchar(256) NOT NULL,
	day integer NOT NULL,
	month integer NOT NULL,
	year integer NOT NULL,
	cases integer,
	deaths integer,
	country varchar(256) NOT NULL,
	alpha_2_code varchar(256) NOT NULL,
	alpha_3_code varchar(256) NOT NULL,   
    population bigint
);

CREATE TABLE public.hospital_beds (
	country_code varchar(256) NOT NULL,
	inticator varchar(256),
	subject varchar(256),
	measure varchar(256),
	freaquency varchar(256),
    year integer,
    hospital_beds_per_1000 numeric(18,0) NOT NULL
);

CREATE TABLE public.population_by_country (
	index integer NOT NULL,
	country varchar(256) NOT NULL,
	alpha_3_code varchar(256) NOT NULL,
	indicator_name varchar(256) NOT NULL,
	indicator_code varchar(256) NOT NULL,
	population numeric(18,0) NOT NULL
);

CREATE TABLE public.covid_19_cases (
	state varchar(256),
	country varchar(256) NOT NULL,
	lat varchar(256),
	long varchar(256),
	date_of_record varchar(256),
	confirmed integer,
	deaths integer,
	recovered integer
);