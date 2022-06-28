### DATA LAKES WITH SPARK

### **Introduction**
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, we will building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
### **Project Description**
In this project, we will build an ETL pipeline for a data lake hosted on S3.
The data will load from S3, process it into analytics tables using Spark, and load them back into S3.

### **Project Datasets**
* *Song data: s3://udacity-dend/song_data*
* *Log data: s3://udacity-dend/log_data*

### **Song Dataset**
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. 
The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

* *song_data/A/B/C/TRABCEI128F424C983.json*
* *song_data/A/A/B/TRAABJL12903CDCF1A.json*


And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like: 
* {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### **Log Dataset**
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

* *log_data/2018/11/2018-11-12-events.json*
* *log_data/2018/11/2018-11-13-events.json*


### **Schema for Song Play Analysis**
Using the song and log datasets, we will need to create a star schema optimized for queries on song play analysis. This includes the following tables.
 #### **Fact Table**:

 - **songplays** - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
 #### **Dimension Tables**:

- users - users in the app
user_id, first_name, last_name, gender, level.
- songs - songs in music database
song_id, title, artist_id, year, duration.
- artists - artists in music database
artist_id, name, location, lattitude, longitude.
- time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday.

### **Project Template**
 The project template includes three files:
- etl.py - reads data from S3, processes that data using Spark, and writes them back to S3.
- dl.cfg - contains your AWS credentials.
- README.md - provides discussion on your process and decisions.
- datalake_local.ipynb -  The jupyter notebook to read data from locally folder,processes that data using Spark, and writes them back to output locally folder.
- unzipDataBase.ipynb - The jupyter notebook to unzip the Zip file dataset to support to test on locally.
- requirement.txt  - contains python library requirements for run the etl.py script on locally.


### **How to run this project on AWS EMR?**
- **Step 1**:  Configure your cluster with the following settings
  - *Release: **emr-5.20.0 or later***
  - *Applications: **Spark**: Spark **2.4.0** on Hadoop **2.8.5** YARN with Ganglia 3.7.2 and Zeppelin 0.8.0*
  - *Instance type: **m3.xlarge***
  - *Number of instance: **3***
  - *EC2 key pair: **Proceed without an EC2 key pair** or feel free to use one if you'd like*
  - *You can keep the remaining default setting and click "Create cluster" on the bottom right.*
- **Step 2**: Wait for Cluster "Waiting" Status
  - *Once you create the cluster, you'll see a status next to your cluster name that says Starting. Wait a short time for this status to change to Waiting before moving on to the next step.*
- **Step 3**: Create Notebook
  - *Now that you launched your cluster successfully, let's create a notebook to run Spark on that cluster.*
  - *Select "Notebooks" in the menu on the left, and click the "Create notebook" button.*
- **Step 4**: Configure your notebook
  - *Enter a name for your notebook*
  - *Select "Choose an existing cluster" and choose the cluster you just created*
  - *Use the default setting for "AWS service role" - this should be "EMR_Notebooks_DefaultRole" or "Create default role" if you haven't done this before.*
  - *keep the remaining default settings and click "Create notebook" on the bottom right.*
- **Step 5**: Wait for Notebook "Ready" Status, Then Open
  - *Once you create an EMR notebook, you'll need to wait a short time before the notebook status changes from Starting or Pending to Ready. Once your notebook status is Ready, click the "Open" button to open the notebook.*
- ### **Start Coding!**


### **How to run this project locally?**
I provided an option to run this project locally, instead of retrieving the data from S3, we can retrieve the data from data file folder.Process it by using Spark lib then store the result back to locally data folder.This is especially useful to iteratively develop your processing logic, without waiting for a long time to load data from S3.

To do that, just change the option data input of **etl.py** file the run it.

Before run **datalake_local.ipynb** for testing, please run the **unZipDataBase.ipynb** to create the data locally folder.
