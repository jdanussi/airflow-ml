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


Asignar direcciones públicas elásticas para los siguientes recursos:
- vpc-airflow-ml-ngw (NAT Gateway de la capa de aplicación)
- bastion-host 
- nginx
- superset-pub

Estas IPs Públicas fijas facilitan la gestión del ambiente de laboratorio que cambia con cada sesión.


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
<br>


## Flujo ETL y Machine Learning

El flujo de ETL se realiza desde la instancia EC2 `airflow` que corre Airflow 2.4.2 sobre un contenedor de docker. 
Como se corre Airflow en forma standalone sobre una instancia y no sobre un cluster, se decidió usar el Local Executor con PostgresSQL como backend porque permite la ejecución simultánea de tareas que aunque no es necesario para este caso, se seteó así para probar.
En contraste, también puede configurarse un Sequential Executor con SQLite como backend o Celery Executor que permite el escalado de los workers dentro de un cluster.


El DAG `etl_ml_pipeline con` se ejecuta anualmente, cada 01/Ene a la 00:00 hs y realiza en secuencia lo siguiente:

1. Se conecta al Data Lake en S3, a la ruta airflow-ml-datalake/01_bronze y baja localmente el archivo .csv que corresponde al año anterior.
2. Con el archivo ubicado en la EC2 se realiza tareas de cleaning y ajuste de esquema. Ej: se eliminan las filas con datos nulos en la columna DEP_DELAY, se renombran columnas, etc. Una vez finalizada la limpieza y transformación, se realiza un upload del dataset refinado en formato parquet a la ubicación **silver** del Data Lake - `airflow-ml-datalake/02_silver/year` - Los datasets ahi almacedos pueden ser útiles para posteriores análisis de datos.
3. Con el dataset refinado - que aun sigue almacedado localmente en la instacia donde corre Airflow - se realiza luego una agregación de los datos promediando el tiempo de demora de la partida de los vuelos por Año y por Origen. Se realiza un upload del dataset con información agregada en formato parquet a la ubicación **gold** del Data Lake `airflow-ml-datalake/03_gold/agg_dep_delay_by_date/year`. Los datasets ahi almacedos pueden ser útiles para presentar en dashboards de análisis.
4. El dataset con información agregada continúa su workflow ingresando a un proceso de Machine Learning para detectar anomalías en el promedio de las demoras. Para la detección de anomaías en series temporales se utilizó el modelo no supervisado Insolation Forest que suele ser efectivo cuando el hiperparámetro de contaminación - porcenje de outliers respecto a los datos totales - es bajo, típicamente un 10% o 0.01. Pueden consultarse los análisis realizados para la elección del modelo en los notebooks ubicados en la carpeta `/notebooks`. 
5. Las anomalías encontradas por el modelo son señalas dentro del dataset, y los datos con esta información incorporada se almacenan en una RDS Postgres que haría el papel de data warehouse para los análisis.
6. Finalmente, la última tarea del DAG es eliminar todos los archivos descargados y generdos durante la corrida. 


## Dashboard de Analísis

La instancia EC2 superset-pub corre Apache Superset sobre un contenedor de Docker, utilizando Postgres como backend.
Superset está conectado a la RDS que almacena los datos procesados por el pipeline ML, y sobre el mismo se creo un dashboard que muestra algunos datos sobre las demoras y las nomalías. 
Se intentó desplegar Apache Superset en la capa de aplicación detrás de Nginx pero no resultó bien porque se generaban errores de timeout en Nginx al tratar de cargar los schemas de la RDS desde el SQL Lab.


## Cuestiones para mejorar

- Utilizar más modelos de Machile Learning y compararlos para utilizar en el pipeline el que sea más eficiente para detectar las anomalías.
- Resolver el problema de Nginx haciendo de proxy a Superset. La aplicación de analítica no tiene porque estar expuesta directamente a internet.
- Implementar SSL en Nginx, en el webserver de Airflow y en Superset, para evitar transmitir credenciales sin encriptar.



