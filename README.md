# IoT Sensor Data Analysis with Spark SQL

This project analyzes IoT sensor readings using Apache Spark and Spark SQL. The data includes temperature and humidity readings from various sensors across different locations and times. Each task involves SQL-style queries, aggregations, window functions, and pivot tables.

---

## Dataset

**File**: `sensor_data.csv`  
**Fields**:
- `sensor_id`: Unique identifier for the sensor
- `timestamp`: Date and time of the reading
- `temperature`: Temperature in °C
- `humidity`: Humidity percentage
- `location`: Sensor’s location (e.g., BuildingA_Floor1)
- `sensor_type`: Sensor hardware type (e.g., TypeA)

---

## Install packages 

1. 
    ```bash 
    pip install pyspark 
    ```

2. 
    ```bash 
    pip install faker
    ```


## Run the data generator

```bash 
python3 data_generator.py
```

It should look like this: 
```bash 
sensor_id,timestamp,temperature,humidity,location,sensor_type

1066,2025-04-05 20:54:00,22.63,32.6,BuildingB_Floor1,TypeC
1077,2025-04-07 00:33:39,31.78,67.93,BuildingA_Floor2,TypeB
1021,2025-04-04 13:31:32,26.05,56.6,BuildingB_Floor2,TypeB
1019,2025-04-06 21:52:08,17.94,57.23,BuildingA_Floor2,TypeC
1040,2025-04-05 15:39:50,19.72,72.1,BuildingB_Floor1,TypeA
1015,2025-04-06 02:27:55,29.1,51.26,BuildingB_Floor1,TypeA
1004,2025-04-07 21:33:20,28.19,69.65,BuildingB_Floor2,TypeC
```

## Run the Iot sensor data file 

Running this file will run the code for all the tasks 

```bash 
python3 Iot_sensor_data.py
```

This will create the following files:

    - task1_output.csv
    - task2_output.csv
    - task3_output.csv
    - task4_output.csv
    - task5_output.csv

task 1 

    - counts the total records and saves all distinct sets of loocations or sensors


task 2 

    - Groups by location and compute the average temperature and average humidity.

task 3 

    - Extracts the hour of the day and groups by hour to find the average temperature

task 4 

    - Compute each sensor’s average temperature over all its readings and orders sensors by average temperature

task 5 

    - Created a pivot table with locations, hour of day and the average temperature. 



## Summary 

Hottest Location/Hour: 

- Building A Floor 1 at hour 20 

always warm: 

- Building A Floor 2 has higher mid-day temp compared to others 

Weird/unsure 

- Building B Floor 1 at hour 17 is a lot cooler then the others 