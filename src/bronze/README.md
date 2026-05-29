# JOB BRONZE - Carga de Datos Empresariales

## Paso 1: Crear el Job de Glue para Bronze

1. Acceda al servicio **AWS Glue**

2. En el menú lateral, seleccione **ETL Jobs**

3. Haga clic en **Script editor**

4. Configure:
   - **Motor:** Spark
   - **Opciones:** Start fresh
   - Haga clic en **Create**

## Paso 2: Configurar los detalles del Job

En la pestaña **Job details**, configure los siguientes parámetros:

**Información básica:**
- **Nombre:** `job-bronze-empresas`
- **Rol de IAM:** `GlueETLMedallionRole`
- **Tipo:** Spark
- **Versión de Glue:** 5.0
- **Lenguaje:** Python 3

**Configuración de recursos:**
- **Tipo de worker:** G.1X
- **Número de workers:** 10
- **Tiempo de espera del trabajo:** 480 minutos

## Paso 3: Agregar parámetros del Job

En la sección **Job parameters**, agregue los siguientes parámetros:

| Clave | Valor |
|-------|-------|
| `--BUCKET` | `glue-bucket-rues-{codigo}` |
| `--BRONZE_PATH` | `s3://glue-bucket-rues-{codigo}/bronze/` |


![img-bz-02](../../public/img-bz-02.png)



**Nota:** Reemplace `{codigo}` con su identificador único (ejemplo: `jc2025`)

## Paso 4: Agregar etiquetas

En la sección **Tags**, agregue:
- **Clave:** `workshop`
- **Valor:** `big_data`

## Paso 5: Guardar la configuración

Haga clic en **Save** para guardar la configuración del Job.

![img-bz-01](../../public/img-bz-01.png)

![img-bz-03](../../public/img-bz-03.png)


# Crawlear Capa bronze
Atención: las imágenes presentadas a continuación corresponden al crawler de la Silver, actualmente las imágenes para capa bronze están en proceso de actualización. Sin embargo, las instrucciones mostradas son correctas para el crawler Bronze.

## Paso 1: Acceder a AWS Glue

1. Acceda al servicio **AWS Glue** en la consola de AWS

2. En el menú lateral, seleccione **Crawlers**

3. Haga clic en **Create crawler**

![img-cr-sil-01](../../public/img-cr-sil-01.png)


## Paso 2: Configurar las propiedades del Crawler

**Set crawler properties:**

1. **Name:** `crawler-bronze-rues`

2. **Description (opcional):** `Crawler para catalogar datos procesados en la capa bornze`

3. **Tags:**
   - Clave: `workshop`
   - Valor: `big_data`

4. Haga clic en **Next**

![img-cr-sil-02](../../public/img-cr-sil-02.png)


## Paso 3: Configurar el origen de datos

**Choose data sources and classifiers:**

1. Haga clic en **Add a data source**

2. Configure el data source:
   - **Data source:** S3
   - **S3 path:** `s3://glue-bucket-rues-{codigo}/bronze/RUES_DATA_SILVER_PARQUET/`
   - **Subsequent crawler runs:** Crawl all sub-folders
   - Deje las demás opciones por defecto

3. Haga clic en **Add an S3 data source**

4. Haga clic en **Next**

![img-cr-sil-03](../../public/img-cr-sil-03.png)
![img-cr-sil-04](../../public/img-cr-sil-04.png)


## Paso 4: Configurar el rol de IAM

**Configure security settings:**

1. **Existing IAM role:** Seleccione `GlueETLMedallionRole`

2. Haga clic en **Next**

![img-cr-sil-05](../../public/img-cr-sil-05.png)

## Paso 5: Configurar el destino (Data Catalog)

**Set output and scheduling:**

1. **Target database:** Seleccione `db_rues`

2. **Table name prefix (opcional):** `bronze_`

3. **Crawler schedule:**
   - **Frequency:** On demand

4. Haga clic en **Next**

![img-cr-sil-06](../../public/img-cr-sil-06.png)


## Paso 6: Revisar y crear

**Review and create:**

1. Revise toda la configuración:
   - Nombre: `crawler-bronze-rues`
   - Data source: `s3://glue-bucket-rues-{codigo}/bronze/RUES_DATA_SILVER_PARQUET/`
   - IAM role: `GlueETLMedallionRole`
   - Target database: `db_rues`
   - Table prefix: `bronze_`

2. Haga clic en **Create crawler**

![img-cr-sil-07](../../public/img-cr-sil-07.png)



## Paso 7: Ejecutar el Crawler

1. En la lista de crawlers, seleccione `crawler-bromze-rues`

2. Haga clic en **Run**

3. Espere a que el estado cambie de **Running** a **Ready**

4. Tiempo estimado: 1-2 minutos

![img-cr-sil-08](../../public/img-cr-sil-08.png)

## Paso 8: Verificar la tabla creada

1. En AWS Glue, vaya a **Data Catalog** > **Tables**

2. Filtre por database: `db_rues`

3. Verifique que exista la tabla:
   - `silver_rues_data_bronze_parquet`

4. Haga clic en la tabla para ver:
   - **Esquema:** Todas las columnas procesadas incluyendo campos derivados
   - **Partition keys:** `year_partition`
   - **Location:** Ruta en S3
   - **Classification:** parquet

![img-cr-sil-09](../../public/img-cr-sil-09.png)