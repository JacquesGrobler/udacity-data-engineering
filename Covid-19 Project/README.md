# Data Engineering Capstone Project

## Table of Contents
- Scope 
- Data Pipeline
- Kaggle Data Sources
- Data Model
- Data Dictionaries
- Technologies Chosen
- Addressing Other Scenarios 
- Instructions to Run the Script

### Scope
The scope of this project includes creating a data piplines which fetches data from Kaggle, pushes the data into s3 and from there pushes the data to Redshift to create tables **for analysis** on the COVID-19 virus. 

### Data Pipeline
The data pipeline built is visually represented in the image below: 

![image](https://user-images.githubusercontent.com/46716252/81408991-914d8d00-913e-11ea-9244-aaf24278c6d2.png)

Apache airflow is used as the workflow manager in the data pipeline which is scheduled to run every morning 7am because some of the data sources get updated daily, the steps include:

(1) Fetching files from Kaggle and storing it on your local machine. An option is available to fetch an entire dataset or specific files in a dataset. Bigger files can be downloaded as zip files. This is done by the KaggleToLocal custom operator found in plugins/operators/import_from_kaggle.py.

(2) Pushing these files to an s3 bucket. This is done by the ExportToS3 custom operator found in plugins/operators/export_to_s3.py.

(3) Deleting any staging tables if they already exists in Redshift. This is done by the CreateOrDeleteOperator custom operator, where the create_or_delete parameter is set to "delete", found in plugins/operators/create_or_delete_tables.py.

(4) Creating staging tables and populating them with data from these files on AWS Redshift. This is done by the CreateOrDeleteOperator custom operator, where the create_or_delete parameter is set to "create", found in plugins/operators/create_or_delete_tables.py and then calling the S3ToRedshiftOperator operator found in plugins/operators/s3_to_redshift.py.

(5) From the staging tables creating the four dimension tables. The steps that were taken to create these tables will be explained in the Data Model section. This is done by the LoadTableOperator custom operator found in plugins/operators/load_table.py.

(6) From these four dimension tables creating the summary fact table. The creation of this table will be explained in the data model section. This is done by the LoadTableOperator custom operator found in plugins/operators/load_table.py.

(7) Doing data quality checks on these tables. This invloves ensuring all table have data in them and are not empty. Other data quality measures include conditions in inserting data into the different tables, such as excluding records that have null values for fields that shouldn't be null and including contraints in the create table statements (found in create_tables.sql). This is done by the DataQualityOperator custom operator found in plugins/operators/data_quality.py.

(8) Deleting the staging tables. This is done by the CreateOrDeleteOperator custom operator, where the create_or_delete parameter is set to "delete", found in plugins/operators/create_or_delete_tables.py.

### Kaggle Data Sources

Kaggle data sources were used to create the tables in the data model. The links to these data sources:
- https://www.kaggle.com/imdevskp/corona-virus-report which has data in CSV format on the amount of cases, deaths and recovered per day. This data source has about 27 200 rows of data.
- https://www.kaggle.com/cristiangarrido/covid19geographicdistributionworldwide, the most important files in this data source are the country ISO file (csv) which has country names and country codes, hospital beds by country (csv) which shows the amount of hospital beds per country and total population by country (csv) which shows the total population for each country. This data source has about 600 rows of data.
- https://www.kaggle.com/aellatif/covid19 this data source has two csv files of tweet data which is about 1 720 000 rows of data.  
- https://www.kaggle.com/juanmah/world-cities this data source has a csv file that shows city names of a country and has about 13 000 rows of data.

### Data Model

The data is modelled as in the image below:

![image](https://user-images.githubusercontent.com/46716252/81139963-0155ea80-8f68-11ea-9ff2-c7f5f28622c2.png)

The tables were modelled in this way to make joining between tables easy for analyses, for example each table has country and country_code fields, which are consistent across all tables, and can be used to join between any of the tables. The steps taken to create each of the tables will now be explained:

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
(a.active_cases::bigint*1000000)/c.population as active_cases_per_1_million,
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
	((t.tweet_count::bigint*100)/(sum(t.tweet_count::bigint) over ()))::real as percentage_of_sample
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

### Data Dictionaries

Below are the data dictionaries for the 5 final tables:

##### covid_19_cases_summary

![image](https://user-images.githubusercontent.com/46716252/81425098-38d6b980-9157-11ea-84ad-6b1e6cfe80e7.png)

##### covid_19_cases_history

![image](https://user-images.githubusercontent.com/46716252/81408286-3d8e7400-913d-11ea-9348-c925f95c0bbd.png)

##### country_tweets

![image](https://user-images.githubusercontent.com/46716252/81408342-5b5bd900-913d-11ea-8290-fc5142766c46.png)

##### country_population

![image](https://user-images.githubusercontent.com/46716252/81408424-7c242e80-913d-11ea-84a8-a33d04d31008.png)

##### country_hospital_beds

![image](https://user-images.githubusercontent.com/46716252/81408487-9bbb5700-913d-11ea-97cc-b774e031cd84.png)

### Technologies Chosen

Apache airflow was chosen as the workflow manager, AWS s3 was chosen for data storage and AWS Redshift was chosen as the data warehouse, where the data is modelled. 

This was chosen because of the easy compatibility between systems and allows for a fully automated process from fetching data in Kaggle to create the final data model. At the moment the total amount of data moving through the pipeline is less than 2 million rows which is handled quite easily by the current pipeline.

### Addressing Other Scenarios 

Below are possible scenarios that could happen and will need to be addressed: 

*The data was increased by 100x:* If the data gets increased by by 100x Apache Spark could be used. Once the files are in s3 Apache Spark can be used to process the data, partition the data by da specific time interval (e.g.day) and send these files back to the s3 bucket, from there Airflow could once again be used to append data for only that particular time interval the the existing tables (the append option is available in the current pipeline tasks).


*The pipelines would be run on a daily basis by 7 am every day:* Airflow provides the scheduling function, so the pipeline can easy be set to run every day 7am. Currently the pipeline shouldn't take more than 20 min to run.


*The database needed to be accessed by 100+ people:* If this is the case each person will need to have his/her own user and password to access the database with each user given appropriate permissions to ensure only certain users can make changes on the database, e.g. create or update tables. Different schemas can be created for the different business needs to make it easier for the users to find needed tables. Amazon Redshift WLM can also be configured to run with automatic WLM, which will maximize system throughput and use resources effectively.

### Instructions to Run the Script

1: Ensure that airflow has been installed and set up, refer to the Airflow Quickstart guide: https://airflow.apache.org/docs/stable/start.html

2: Install the Kaggle Client:
`pip3 install kaggle`

3: Generate API Tokens. To do this create an account on Kaggle, go to your account and find the API section:

![image](https://user-images.githubusercontent.com/46716252/81373283-a4d80400-90fc-11ea-9ace-a5b44c5eb420.png)

Select "Create New API Token".This downloads a kaggle.json file with the API tokens in it. This file needs to be moved to – ~/.kaggle/kaggle.json.

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
- **s3_conn:** connection type "S3" and in the extras field paste your AWS admin user access Key ID and Secret Access Key in json format, it should look like this: {"aws_access_key_id":"XXXXXXXXXXXXXXXXXXXX", "aws_secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}
- **aws_credentials:** connection type "Amazon Web Services" and paste your AWS admin user access Key ID in the login fields and secret Access Key in the password field.
- **redshift:** connection type Postgres, the host is the endpoint (without the port and database at the end), schema is the database name and then add the database user and password that was created when creating the cluster, finally add the port number which would be 5439.

7: Ensure that the correct bucket name is entered in all paramters in the DAG (Lines 44, 68, 81, 94, 107, 120, 133, 146).

8: (Optional), if you intend to download the Kaggle data to a different path then that needs to be specified in lines 37 and 43. 
	
9: Run the dag.
