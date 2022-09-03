# XML-to-CSV
Convert XML Doc to CSV


# Data Warehouse
***
First, we need to get a look on the original data in the buckets. To do so, we will read the data into DataFrames and see what it the columns and data types we have from the notebook: **Access_Data_From_Buckets**:
* First we need to pip install **s3fs** to directly read the data from the buckets into a dataframe
* For the Song Data, we will have those columns: -
    - artist_id , 
    - artist_latitude , 
    - artist_locatio, 
    - artist_longitude, 
    - artist_name, 
    - duration
    -  num_songs, 
    - song_id, 
    - title, 
    - year.
* for Log Data, we will have those columns: 
    - artist            object
    - auth              object
    - firstName         object
    - gender            object
    - itemInSession      int64- 
    - lastName          object
    - length           float64
    - level             object
    - location          object
    - method            object
    - page              object
    - registration     float64
    - sessionId          int64
    - song              object
    - status             int64
    - ts                 int64
    - userAgent         object
    - userId            object	
 * we want to convert this data into Star Schema to start building our Warehouse.
 ***
### Star Schema
 ***
![image](https://user-images.githubusercontent.com/29911679/187971714-ae264d5d-6279-4a0b-8680-ce19267bb854.png)

***
#### Fact Table:
- songplays
#### Dimension Tables:
- users
- artists
- time
- songs

##### Now we want to build our tables in sql_queries, each table and each staging table needs to be built from scratch.

***
After we finished the tables in sql_queries, we need to create the cluster and IAM Role, etc. we will run the notebook IaC with setting the initial access keys in dwh.cfg file.

***
After the cluster is created, we will run
1- python3 create_tables.py
2- python3 etl.py
