# Project for VK

### At first we need to run docker:

```
docker compose up --build -d
```

### after docker started, Airflow UI can be found at:

```
localhost:8080
Username: admin
Password: admin
```

### Before turning on the DAG, Spark connection should be set:

```
Connection Id: spark-conn
Connection Type: Spark
Host: spark://<IP>
Port: 7077
```

### IP for host can be found at localhost:9090

### after catchup manual execution of a dug needed to get weekly report for today

### No need to generate input beforehand, input is automatically generated in DAG to simulate real life situation in wich we need to download new data each day
