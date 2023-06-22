# LAClippersDashboard
Table of Contents
Introduction
Pipeline
Environment Set Up
Repository Structure
Development Work
Future Enhancements
Screenshots

Introduction
LA Clippers Dashboard: Tracking the Team’s Performance

The goal of this project is to showcase the performance of my favorite NBA team, the Los Angeles Clippers in an easy to read format.

In this project, I built a pipeline to use an unofficial NBA API that exposes the endpoints in the official NBA website. The endpoints can be used to pull up to date data from the NBA. I’ve used these endpoints to set up recurring scripts to pull data daily from the NBA. This data is then transformed and reduced down to the essentials. Screenshots from the dashboard I’ve designed are available below.


Pipeline
I built a data pipeline that utilizes the nba_api repo by swar (Github) to access official NBA data.

Python scripts to pull the data from the NBA API and store it locally in Panda dataframes.
The data is formatted and cleaned through Python scripts and Pandas.
The data is then stored in a MySQL database through AWS RDS.
Visualizations are created through Metabase.

Languages
Python 3.7
MySQL

Technologies
AWS RDS
Metabase

Third-Party Libraries
nba_api
Pandas

Repository Structure
./populate_tables/ contains the scripts for the data pipeline for each table, split into backfill scripts and daily scripts for use with a scheduler.

./utils/ contains commonly used functions for extracting, transforming, and loading data from the API into the database.

./main.py/ has developmental code which was then moved to the populate_tables scripts.


Development Work
I initially was going to utilize Streamlit to visualize the data snd to be exposed to new Python visualization libraries. After some testing, I decied to pivot to plotly and dash as I had more experience with those libraries. After seeing how much tweaking could potentially come up, I then converted to using Metabase to visualize my data as it required the least amount of setup and maintenance on my part and was more focused on the results.

I did my database testing locally through DBeaver on a local MySQL database. Once the table structure was finalized, all data was stored on an AWS RDS database.

All dashboards were validated through the local MySQL database and then through a connection to the AWS database through Metabase.

Languages
Python

Technologies
AWS RDS
Metabase

Future Enhancements
Making the Metabase dashboard public would be the first enhancement that could be made. My initial concern for this is access control and how many reads could be done on the AWS database due to potential cost.

Adding more advanced metrics to my database is another enhancement I would love to add, such as Win Shares. This could open the door to more advanced analysis but was left off this version due to concerns of being underutilized and cluttering the current metrics on display.

I would love to add a heat chart for shots taken on a chart of a basketball court. This is a bit more of a technical undertaking but would be a very nice visual to see.

Originally, I planned to use AirFlow to run daily scripts to update my AWS database to keep the data up to date. Due to finishing this project during the NBA offseason, I’ve decided to hold off on this part until the NBA season starts up again in the Fall.

Screenshots

![Screen Shot 2023-05-29 at 6 02 06 PM](https://github.com/cristian-franco/LAClippersDashboard/assets/40413416/3ea7926f-e1c6-49d8-84ce-7ea12b742d9d)


![Screen Shot 2023-05-29 at 6 02 55 PM](https://github.com/cristian-franco/LAClippersDashboard/assets/40413416/9f1cf78d-e543-45e9-821f-5989b9a26184)



![Screen Shot 2023-05-29 at 6 03 18 PM](https://github.com/cristian-franco/LAClippersDashboard/assets/40413416/5ae015a4-c633-4fa2-92bb-8a2ad32ca49d)



