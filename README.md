# Project for VK

### At first we need to run docker:

```
docker compose up --build -d
```

### after docker started, Airflow UI can be found at:

```
localhost:8080
```

### Before turning on the DAG, Spark connection should be set:

```
Connection Id: spark-conn
Connection Type: Spark
Host: spark://report-generation-with-airflow-and-spark-spark-master-1
Port: 7077
```

### No need to generate input beforehand, input is automatically generated in DAG to simulate real life situation in wich we need to download new data each day
