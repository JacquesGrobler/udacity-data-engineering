# Data Engineering Capstone Project

## Table of Contents
- Scope
- Data Pipeline
- Kaggle Data Sources
- Data Model
- Why Airflow
- Addressing Other Scenarios 
- Instructions to Run the Script

### Scope
The scope of this project includes creating a data piplines which fetches data from Kaggle, pushes the data into s3 and from there pushes the data to Redshift to create tables **for analysis** on the COVID-19 virus. 

### Data Pipeline and Tools Used
The data pipeline built is visually represented in the image below: 

![image](https://user-images.githubusercontent.com/46716252/81370794-50ca2100-90f6-11ea-94c5-177acb4a6177.png)

The steps include:
(1) Fetching files from Kaggle and storing it on your local machine. An option is available to fetch an entire dataset or specific files in a dataset. Bigger files can be downloaded as zip files. 
(2) Pushing these files to an s3 bucket.
(3) Deleting any staging tables if they already exists in Redshift.
(4) Creating staging tables from these files on AWS Redshift.
(5) From the staging tables creating the four dimension tables. The steps that were taken to create these tables will be explained in the Data Model section.
(6) From these four dimension tables creating the summary fact table. The creation of this table will be explained in the data model section. 
(7) Doing data quality checks on these tables. This invloves ensuring all table have data in them and are not empty. Other data quality measures include conditions in inserting data into the different tables, such as excluding records that have null values for fields that shouldn't be null.
(8) Deleting the staging tables.

### Kaggle Data Sources

Kaggle data sources were used to create the tables in the data model. The links to these data sources:
- https://www.kaggle.com/imdevskp/corona-virus-report which has data in CSV format on the amount of cases, deaths and recovered per day. This data source has about 27 200 rows of data.
- https://www.kaggle.com/cristiangarrido/covid19geographicdistributionworldwide, the most important files in this data source are the country ISO file (csv) which has country names and country codes, hospital beds by country (csv) which shows the amount of hospital beds per country and total population by country (csv) which shows the total population for each country. This data source has about 600 rows of data.
- https://www.kaggle.com/aellatif/covid19 this data source has two csv files of tweet data which is about 1 720 000 rows of data.  
- https://www.kaggle.com/juanmah/world-cities this data source has a csv file that shows city names of a country and has about 13 000 rows of data.

### Data Model

The data is modelled as in the image below:

![image](https://user-images.githubusercontent.com/46716252/81139963-0155ea80-8f68-11ea-9ff2-c7f5f28622c2.png)

The tables were modelled in this way to make joining between tables easy for analyses, for example each table has country and country_code fields, which are consistent accross all tables, and can be used to join between any of the tables. The steps taken to create each of the tables will now be explained:

##### covid_19_cases_summary

covid_19_cases_summary is created using covid_19_cases_history as the base table and using only the latest day's values. The other tables are all then joined on the country column to add additional information. See create_tables.sql to see the constraints on the different columns for the table. The SQL to insert data into covid_19_cases_history (can also be found in airflow/plugins/helpers.sql_queries.py):

```
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
```

##### covid_19_cases_history

covid_19_cases_staging is used as the base table, country_iso_stage is then joined to it to include country and country_code  (country_iso_stage is joined to all of the dimension tables to ensure that these values remain consistent and to ensure ease of joining between the tables). Conditions are then added to exclude records where country is null and to exclude records where date_of_record is null, to ensure data quality. See create_tables.sql to see the constraints on the different columns for the table. The ERD for this tables can be seen below:

![image](https://user-images.githubusercontent.com/46716252/81256389-afc56280-9030-11ea-8287-d597f7bbb1a6.png)

The SQL to insert data into covid_19_cases_history (can also be found in airflow/plugins/helpers.sql_queries.py):

```
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
```

##### country_tweets

tweets_stage is used as the base table which is first joined to cities_stage to get the country and iso value from the user_location field in tweets_stage. country_iso_stage is then joined to ensure that the country and country_code fields are consistent with the other tables. A coundition is added to ensure no records are included where the country value is null. See create_tables.sql to see the constraints on the different columns for the table. The ERD for this tables can be seen below:

![image](https://user-images.githubusercontent.com/46716252/81256418-be137e80-9030-11ea-9f09-c37947ceeb04.png)

The SQL to insert data into covid_19_cases_history (can also be found in airflow/plugins/helpers.sql_queries.py):

```
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
```

##### country_population

population_by_country_stage is used as the base table, country_iso_stage is then joined to ensure that the country and country_code fields are consistent with the other tables. Conditions are added which exclude any records where population, alpha_3_code or country is null, to ensure data quality. See create_tables.sql to see the constraints on the different columns for the table. The ERD for this tables can be seen below:

![image](https://user-images.githubusercontent.com/46716252/81256411-b9e76100-9030-11ea-9266-44b9e011e9d0.png)

The SQL to insert data into covid_19_cases_history (can also be found in airflow/plugins/helpers.sql_queries.py):

```
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
```

##### country_hospital_beds

hospital_beds_stage is used as the base table, population_by_country_stage is joined to work out the total number of beds per country and country_iso_stage is joined to ensure that the country and country_code fields are consistent with the other tables. Conditions are added to exclude any records where country, country_code or hospital_beds_per_1000 are null. See create_tables.sql to see the constraints on the different columns for the table. The ERD for this tables can be seen below:

![image](https://user-images.githubusercontent.com/46716252/81256402-b5bb4380-9030-11ea-90c0-01c8fd9dd326.png)


The SQL to insert data into covid_19_cases_history (can also be found in airflow/plugins/helpers.sql_queries.py):

```
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
```

### Why Airflow

### Addressing Other Scenarios 

### Instructions to Run the Script

1: Ensure that airflow has been installed and set up, refer the the Airflow Quickstart guide: https://airflow.apache.org/docs/stable/start.html

2: Install the Kaggle Client:
`pip3 install kaggle`

3: Generate API Tokens. To do this create an account on Kaggle, go to your account and find the API section:

![image](https://user-images.githubusercontent.com/46716252/81373283-a4d80400-90fc-11ea-9ace-a5b44c5eb420.png)

Select "Create New API Token".This downloads a kaggle.json file with the API tokens in it. We need to move this file to – ~/.kaggle/kaggle.json.

**Note:** Please make sure to not share the tokens with anyone. These are secrets.

To create a folder in your home directory for the kaggle.json file, and move the file there:
```
mkdir ~/.kaggle
mv <source path> <destination path>
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
```

You might get this error if the file permissions are too relaxed:
*Your kaggle API key is readable by other users on this system.*

To fix this issue you can run following command:

```
chmod 600 ~/.kaggle/kaggle.json
```

4: Create a redshift cluster, ensure that the cluster is in the same region as the s3 bucket that you are going to use.

5: Once the cluster is created run the CREATE TABLE scripts in the create_tables.sql file.

6: Launch Airflow and add the necessary connections:
	- **s3_conn:** connection type "S3" and in the extras field paste you AWS admin user access Key ID and Secret Access Key in json format, it should look like this: {"aws_access_key_id":"XXXXXXXXXXXXXXXXXXXX", "aws_secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
	- **aws_credentials:** connection type "Amazon Web Services" and paste your AWS admin user access Key ID in the login fields and secret Access Key in the password field.
	- **redshift:** connection type Postgres, the host is the endpoint (without the port and database at the end), schema is the database name and then add the database user and password that was created when creating the cluster, finally add the port number which would be 5439.
	
7: Run the dag.
