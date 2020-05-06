# Data Engineering Capstone Project

## Table of Contents
- Scope
- Data Pipeline
- Data Model
- Instructions to Run the Script
- Summary of each function in create_tables.py
- Summary of each function in etl.py

### Scope
The purpose of this project is to create a data pipeline that fetches data from Kaggle, pushes the data into s3 and from there pushes the data to Redshift to create tables **for analysis** on the COVID-19 virus. These tables include: 
- A case history table which will show the amount of cases, deaths and recovered per day per country (covid_19_cases_history)
- A country population table which shows the population per country (country_population)
- A table which show the amount of hospital beds per country (country_hospital_beds)
- A table which shows a sample of tweets per country around the time that the virus was declared a pandemic (country_tweets)
- A case summary table which shows the latest cases, deaths and recovered, the amount of hospital beds used, cases per 1 million of the population and the percentage of sample tweets from that country (covid_19_cases_summary).

Kaggle data sources were used to create the above mentioned tables. The links to these data sources:
- https://www.kaggle.com/imdevskp/corona-virus-report which has data in CSV format on the amount of cases, deaths and recovered per day. This data source has about 27 200 rows of data.
- https://www.kaggle.com/cristiangarrido/covid19geographicdistributionworldwide, the most important files in this data source are the country ISO file (csv) which has country names and country codes, hospital beds by country (csv) which shows the amount of hospital beds per country and total population by country (csv) which shows the total population for each country. This data source has about 600 rows of data.
- https://www.kaggle.com/aellatif/covid19 this data source has two csv files of tweet data which is about 1 720 000 rows of data.  
- https://www.kaggle.com/juanmah/world-cities this data source has a csv file that shows city names of a country and has about 13 000 rows of data.

To create the five tables mentioned above staging tables first needed to be created, from the staging tables the first four tables were created (covid_19_cases_history, country_population, country_hospital_beds, country_tweets), from these tables the summary table (covid_19_cases_summary) was created. These steps are expained in more detail below.

### Data Pipeline

![image](https://user-images.githubusercontent.com/46716252/81141465-0701ff00-8f6d-11ea-8900-439da5069d70.png)

### Data Model

The data is modelled as in the image below:

![image](https://user-images.githubusercontent.com/46716252/81139963-0155ea80-8f68-11ea-9ff2-c7f5f28622c2.png)

The tables were modelled in this way to make joing between tables easy, for example each table has country and country_code fields which can be used to join between any of the tables.
