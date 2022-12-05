# Data Pipeline con Machine Learning usando Airflow sobre EC2 y RDS Postgres  

## Resumen

Se desarrolló un data pipeline utilizando Airflow, que extrae datos raw (bronze bucket) del Data Lake implementado en S3, realiza el cleaning de los datos y ajustes de esquema almacenando el resultado en el silver bucket y finalmente realiza una agregación simple de los datos almacenando el resultado en el bucker gold.
Se utilizó para este desarrollo el dataset de Kaggle de demoras y cancelaciones de vuelos en USA entre 2009 y 2018: Airline Delay and Cancellation Data, 2009 - 2018 ( https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018 )


## Diagrama de infraestructura

![diagrama](images/airflow-ml_160.png)  
<br>


# Descripción de infraestructura

A continuación se describen los recursos que deben ser creados para poder utilizar la aplicación.

## Data Lake

Crear el bucket S3 "airflow-ml-datalake" o utilizar cualquier otro nombre que este disponible. El nombre que se seleccione en este paso luego se indica en el archivo de variables que utiliza Airflow para la corrida de las tareas.

En el backet crear las siguientes carpetas:
- airflow-ml-datalake/01_bronze
- airflow-ml-datalake/02_silver/year
- airflow-ml-datalake/03_gold/agg_dep_delay_by_date/year


## Network

Crear VPC CIDR 10.0.0.0/16 que incluya los siguientes recusos:

- Internet Gateway

### Capa Pública:

2 Subnets públicas (para implementar HA en un futuro):
- public-subnet-1: 10.0.1.0/24
- public-subnet-2: 10.0.2.0/24

Tabla de rutas pública:
- public-rt

La tabla de rutas se debe asociar a la 2 subnets públicas y debe incorporar la ruta de salida a internet a travez del Internet Gateway
0.0.0.0/0   Internet Gateway

### Capa de Aplicación:

2 Subnets privadas con salida a internet vía NAT (para implementar HA en un futuro)
- private-subnet-1: 10.0.3.0/24
- private-subnet-1: 10.0.4.0/24

NAT Gateway en alguna de las subnets privadas dadas de alta en el punto anterior.

Tabla de rutas pública:
- private-rt

La tabla de rutas private-rt se debe asociar a las 2 subnets privadas y debe incorporar una ruta de default gateway vía NAT Gateway
0.0.0.0/0   NAT Gataway

Se incorpora NAT Gataway para permitir el download de las imágenes docker que utilizará la instancia EC2 de Airflow a deployar en esta capa.

### Capa de Base de Datos:

2 Subnets privadas sin salida a internet (para implementar HA en un futuro)

- private-subnet-3: 10.0.5.0/24
- private-subnet-4: 10.0.6.0/24

RDS subnet group:
- vpc-airflow-ml-subnets-group que incluya las 2 subnets anteriores. 

En este caso no hace falta cambiar las tablas de rutas asociadas a las subnets (son las default de la VPC) aunque puede ser aconsejable por razones de seguridad, ya que cualquier cambio en la tabla de rutas default genera una brecha de seguridad en la capa de base de datos.


## Compute

Crear las siguientes instancias EC2 en las capas que se indican, la subnet elegida en cada capa es indistinta.

En capa pública:

- bastion-host: EC2 tipo t2.micro que se utiliza como punto de acceso público por ssh a la infraestructura privada asi como también a la RDS.
- nginx: EC2 tipo t2.micro que se uliza como proxy reverso para acceder a la UI de Airflow (en capa de aplicación).
- superset: Instancia tipo t3.large que se utiliza para servir los dashboards de análisis.

En capa de aplicación:

- airflow: Instancia tipo r5.large (Memory Optimized) para correr Airflow sobre docker.

En capa de base de datos:

- database-ml: RDS de tipo db.t3.micro con base de datos inicial `ml_resuls`. Si la base no se crea inicialmente con la RDS, se puede crear luego.


## Seguridad

Los grupos de seguridad de cada recurso deben ser seteados con los accesos mínimos requeridos (buena práctica!). 
Para ello se setean las reglas inbound de los security groups según se indica a continuación.
No se setean reglas outbound ni tampoco se configuran los Network ACLs (se deja el default en ambos casos lo que permite todo el tráfico)

Reglas Inbound de los Security Groups:

- bastion-host-sg: 
    - Se permite acceso ssh desde cualquier origen 0.0.0.0/0.

- superset-pub-sg: 
    - Se permite acceso ssh solo desde bastion-host
    - Se permite acceso a puerto 8088 desde cualquier origen 0.0.0.0/0

- nginx-sg:
    - Se permite acceso ssh solo desde bastion-host
    - Se permite acceso a puerto HTTP desde cualquier origen 0.0.0.0/0

- airflow-sg:
    - Se permite acceso ssh solo desde bastion-host
    - Se permite acceso a puerto 8080 solo desde nginx

- database-ml-sg:
    - Se permite a Postgres TCP 5432 a bastion-host
    - Se permite a Postgres TCP 5432 a airflow
    - Se permite a Postgres TCP 5432 a superset-pub


## Flujo ETL

Se configuró el DAG de Airflow etl_ml_pipeline con schedule anual  

