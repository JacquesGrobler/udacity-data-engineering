class SqlQueries:
    
    create_country_iso = ("""
        CREATE TABLE IF NOT EXISTS public.country_iso_stage (
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
    
    delete_country_iso = "DETELE TABLE IF EXISTS public.country_iso_stage;"
    delete_geographic_disbtribution = "DETELE TABLE IF EXISTS public.covid_19_geographic_disbtribution_stage;"
    delete_hospital_beds = "DETELE TABLE IF EXISTS public.hospital_beds_stage;"
    delete_population_by_country = "DETELE TABLE IF EXISTS public.population_by_country_stage;"  
    delete_covid_19_cases = "DETELE TABLE IF EXISTS public.covid_19_cases_stage;" 
    
    create_table_queries = [create_country_iso, create_country_geographic_disbtribution, create_hospital_beds, create_population_by_country, create_covid_19_cases]
    delete_table_queries = [delete_country_iso, delete_geographic_disbtribution, delete_hospital_beds, delete_population_by_country, delete_covid_19_cases]
                          
    