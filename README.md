## Introduce

This project is used to process weather-related data, and analyze and display these data based on [Apache Horaedb](https://github.com/apache/horaedb).

## Data source

- [Weather Data](https://data.tpdc.ac.cn/en/data/8028b944-daaa-4511-8769-965612652c49/)
- [Air Quality Data](https://www.geodata.cn/data/datadetails.html?dataguid=182018107366091&docid=36)

## Process Data

```
python ./process/aqi/qpi.py
```

There is already processed data in the data directory, you can use it directly.

## Write into HoraeDB

```
python ./db/aqi/qpi.py
```

You can refer to [Apache Horaedb User Guide](https://horaedb.apache.org/docs/getting-started/) to create a Horaedb locally.

## Prometheus

If you need to use promql to query data, you need to start a promtheus service locally and configure the remote read interface.

```
remote_read:
  - url: "http://127.0.0.1:5440/prom/v1/read"
    read_recent: true
```

## Grafana

We use grafana to display weather data.
You need to configure data sources on grafana to connect to Horaedb.

```
sql
127.0.0.1:3307

promtheus
127.0.0.1:9090
```

There is a dashboard in the grafana directory that you can use directly. Of course, you can also create your own dashboard according to your preferences.
