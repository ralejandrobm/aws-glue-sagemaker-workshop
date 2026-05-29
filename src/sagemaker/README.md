# 🏢 Perfiles Empresariales con K-Means — RUES + AWS

> Flujo completo para segmentar empresas del Registro Único Empresarial y Social (RUES) usando Amazon Athena, SageMaker Studio y K-Means.

---

## 📋 Tabla de Contenidos

1. [Arquitectura general](#arquitectura-general)
2. [Pre-requisitos](#pre-requisitos)
3. [Paso 1 — Consulta en Athena](#paso-1--consulta-en-athena)
4. [Paso 2 — Configurar SageMaker Studio](#paso-2--configurar-sagemaker-studio)
5. [Paso 3 — Leer datos desde Athena](#paso-3--leer-datos-desde-athena)
6. [Paso 4 — Preparar datos para K-Means](#paso-4--preparar-datos-para-k-means)
7. [Paso 5 — Elegir número de clusters](#paso-5--elegir-número-de-clusters)
8. [Paso 6 — Entrenar K-Means](#paso-6--entrenar-k-means)
9. [Paso 7 — Interpretar perfiles](#paso-7--interpretar-perfiles)
10. [Paso 8 — Guardar modelo en S3](#paso-8--guardar-modelo-en-s3)
11. [Paso 9 — Crear script de inferencia](#paso-9--crear-script-de-inferencia)
12. [Paso 10 — Desplegar endpoint en SageMaker](#paso-10--desplegar-endpoint-en-sagemaker)
13. [Paso 11 — Probar el endpoint](#paso-11--probar-el-endpoint)
14. [Paso 12 — Exponer como API REST (opcional)](#paso-12--exponer-como-api-rest-opcional)
15. [Costos y limpieza](#costos-y-limpieza)
16. [Estructura del proyecto](#estructura-del-proyecto)

---

## Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                          AWS Cloud                              │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌─────────────────────────┐   │
│  │  Athena  │───▶│   S3     │───▶│    SageMaker Studio     │   │
│  │  (SQL)   │    │ (datos)  │    │  ┌─────────────────────┐│   │
│  └──────────┘    └──────────┘    │  │ Notebook (K-Means)  ││   │
│       │                          │  └─────────────────────┘│   │
│  ┌──────────┐                    │  ┌─────────────────────┐│   │
│  │   Glue   │                    │  │   Model Registry    ││   │
│  │(Catálogo)│                    │  └─────────────────────┘│   │
│  └──────────┘                    │  ┌─────────────────────┐│   │
│                                  │  │  Endpoint (REST)    ││   │
│                                  │  └─────────────────────┘│   │
│                                  └─────────────────────────┘   │
│                                             │                   │
│                                  ┌──────────▼──────────┐       │
│                                  │  API Gateway + Lambda│       │
│                                  └─────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Pre-requisitos

| Requisito | Detalle |
|---|---|
| Cuenta AWS activa | Con permisos en Athena, S3, SageMaker, IAM |
| Bucket S3 | Un bucket existente para resultados y modelos |
| Base de datos Glue | `db_rues` con tablas `gold_dim_empresa` y `gold_fact_renovacion` |
| Rol IAM | Con políticas: `AmazonSageMakerFullAccess`, `AmazonAthenaFullAccess`, `AmazonS3FullAccess` |
|actualizar politica del rol | revisar paso 0| 
| Tutorial base | [big-data-processing-with-aws-glue-workshop](https://github.com/Doc-UP-AlejandroJaimes/big-data-processing-with-aws-glue-workshop) |

---

### paso 0 ⚠️ Configuración del Trust Policy del rol IAM
 
Si usas un rol existente (por ejemplo `GlueETLMedallionRole`) para ejecutar el JupyterLab Space en SageMaker, debes asegurarte de que el rol **confíe en SageMaker**. De lo contrario obtendrás el error `Error acquiring credentials` al abrir Studio.
 
Ve a **IAM → Roles → [nombre del rol] → Trust relationships → Edit trust policy** y verifica que contenga ambos servicios:
 
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "sagemaker.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```
 
> **Nota:** Si prefieres no modificar el rol de Glue, usa el rol `AmazonSageMaker-ExecutionRole-*` que AWS crea automáticamente al configurar Studio y agrégale las políticas `AmazonAthenaFullAccess`, `AWSGlueConsoleFullAccess` y `AmazonS3FullAccess` desde IAM.
 
---

## Paso 1 — Consulta en Athena

Esta consulta genera el dataset base para el modelo. Se puede ejecutar directamente en la consola de Athena o desde el notebook.

```sql
WITH base_join AS (
    SELECT
        d.matricula,
        d.codigo_camara,
        d.camara_comercio,
        d.tipo_sociedad,
        d.organizacion_juridica,
        d.categoria_matricula,
        d.actividad_economica,
        d.tipo_persona,
        f.estado_matricula,
        d.antiguedad_empresa,
        CAST(f.ultimo_ano_renovado AS bigint) AS ultimo_ano_renovado,
        f.fecha_vigencia,
        f.fecha_renovacion,
        f.fecha_actualizacion
    FROM "db_rues"."gold_dim_empresa" AS d
    INNER JOIN "db_rues"."gold_fact_renovacion" AS f
        ON d.matricula = f.matricula
    WHERE
        f.estado_matricula IN ('ACTIVA', 'RENOVADA', 'CANCELADA')
        AND d.antiguedad_empresa IS NOT NULL
        AND f.ultimo_ano_renovado IS NOT NULL
        AND d.tipo_sociedad IS NOT NULL
        AND d.actividad_economica IS NOT NULL
),

deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY matricula ORDER BY fecha_actualizacion DESC) AS rn
    FROM base_join
),

datos_limpios AS (
    SELECT *,
        CAST(antiguedad_empresa AS double) AS antiguedad_clean,
        CAST(YEAR(CURRENT_DATE) AS bigint) - ultimo_ano_renovado AS anos_sin_renovar,
        CASE
            WHEN antiguedad_empresa < 2 THEN 'Nueva'
            WHEN antiguedad_empresa BETWEEN 2 AND 5 THEN 'Joven'
            WHEN antiguedad_empresa BETWEEN 6 AND 10 THEN 'Establecida'
            ELSE 'Madura'
        END AS segmento_antiguedad
    FROM deduplicados
    WHERE rn = 1
)

SELECT
    tipo_sociedad,
    organizacion_juridica,
    actividad_economica,
    segmento_antiguedad,
    antiguedad_clean       AS antiguedad_empresa,
    anos_sin_renovar,
    camara_comercio,
    codigo_camara
FROM datos_limpios
ORDER BY RAND()
LIMIT 100000;
```

---

## Paso 2 — Configurar SageMaker Studio

1. Ir a **AWS Console → SageMaker → Studio**
2. Crear o abrir un dominio existente
3. Lanzar **Studio** con el usuario configurado
4. Slelecciona JupiterLab
5. Crear un nuevo JupiterLabSpace con instancia `ml.t3.medium` *(económica para desarrollo)* 
6. Abre jupiter lab y Seleccionar kernel  `python3 (ipykernel)`.


> **Tip:** Usa el mismo rol IAM que configuraste en el taller de Glue para no tener problemas de permisos.

---

## Paso 3 — Leer datos desde Athena
```python
# Instalar dependencias
!pip install awswrangler scikit-learn matplotlib seaborn joblib -q
!pip install sagemaker==2.232.2 -q
```
# IMPORTANTE REINICIAR EL KERNEL PARA TOMAR LAS NUEVAS INSTALACIONES
### configuración
```python


import awswrangler as wr
import pandas as pd
import numpy as np

# ─── Configuración ───────────────────────────────────────────────
BUCKET   = 'tu-bucket'           # reemplaza con tu bucket
DATABASE = 'db_rues'
S3_OUT   = f's3://{BUCKET}/athena-results/'

# ─── Leer desde Athena ───────────────────────────────────────────
QUERY = """
    -- Pega aquí la consulta del Paso 1
"""

df = wr.athena.read_sql_query(
    sql=QUERY,
    database=DATABASE,
    ctas_approach=True,          # más rápido para grandes volúmenes
    s3_output=S3_OUT
)

print(f"✅ Registros cargados: {df.shape[0]:,}")
print(f"   Columnas: {list(df.columns)}")
df.head()
```

---

## Paso 4 — Preparar datos para K-Means

K-Means solo acepta valores numéricos. Se codifican las variables categóricas y se escalan todas las variables.

```python
from sklearn.preprocessing import LabelEncoder, StandardScaler

# Columnas para el modelo
FEATURES = [
    'tipo_sociedad',
    'organizacion_juridica',
    'actividad_economica',
    'segmento_antiguedad',
    'antiguedad_empresa',
    'anos_sin_renovar'
]

CATEGORICAS = [
    'tipo_sociedad',
    'organizacion_juridica',
    'actividad_economica',
    'segmento_antiguedad'
]

df_model = df[FEATURES].dropna().copy()

# Codificar categóricas
encoders = {}
for col in CATEGORICAS:
    le = LabelEncoder()
    df_model[col] = le.fit_transform(df_model[col].astype(str))
    encoders[col] = le  # guardar para inferencia posterior

# Escalar todas las variables
scaler = StandardScaler()
X = scaler.fit_transform(df_model)

print(f"✅ Matriz lista para K-Means: {X.shape}")
```

---

## Paso 5 — Elegir número de clusters

```python
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

inertias   = []
silhouettes = []
K_RANGE    = range(2, 10)

for k in K_RANGE:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X)
    inertias.append(km.inertia_)

# Gráfica del codo
plt.figure(figsize=(8, 4))
plt.plot(K_RANGE, inertias, marker='o', color='steelblue', linewidth=2)
plt.xlabel('Número de clusters (k)')
plt.ylabel('Inercia')
plt.title('Método del Codo — Selección de K óptimo')
plt.grid(True, alpha=0.4)
plt.tight_layout()
plt.savefig('elbow_curve.png', dpi=150)
plt.show()
```

> **¿Cómo elegir K?** Busca el punto donde la curva "dobla" o deja de bajar abruptamente. Para datos empresariales RUES generalmente el óptimo está entre **3 y 5**.

---

## Paso 6 — Entrenar K-Means

```python
K_OPTIMO = 4  # ajusta según la gráfica del codo

km_final = KMeans(
    n_clusters=K_OPTIMO,
    random_state=42,
    n_init=10,
    max_iter=300
)

df_model['cluster'] = km_final.fit_predict(X)

# Distribución por cluster
print("Distribución de empresas por cluster:")
print(df_model['cluster'].value_counts().sort_index())
```

---

## Paso 7 — Interpretar perfiles

```python
# Pegar cluster al dataframe original
df_resultado = df[FEATURES].dropna().copy()
df_resultado['cluster'] = df_model['cluster'].values

# Resumen estadístico por cluster
perfil = df_resultado.groupby('cluster').agg(
    total_empresas=('antiguedad_empresa', 'count'),
    antiguedad_promedio=('antiguedad_empresa', 'mean'),
    anos_sin_renovar_prom=('anos_sin_renovar', 'mean'),
    tipo_sociedad_frecuente=('tipo_sociedad', lambda x: x.value_counts().index[0]),
    actividad_frecuente=('actividad_economica', lambda x: x.value_counts().index[0]),
    segmento_frecuente=('segmento_antiguedad', lambda x: x.value_counts().index[0])
).round(2)

print(perfil)
```

### Ejemplo de perfiles resultantes

| Cluster | Perfil sugerido | Característica principal |
|---|---|---|
| 0 | 🟢 Empresas nuevas al día | Jóvenes, renuevan puntualmente |
| 1 | 🔴 Empresas maduras en riesgo | Antiguas, muchos años sin renovar |
| 2 | 🟡 Empresas estables | Establecidas, renovación regular |
| 3 | 🟠 Empresas nuevas abandonadas | Recientes, ya dejaron de renovar |

```python
# Asignar nombres a los clusters manualmente
nombres_cluster = {
    0: 'Nuevas al día',
    1: 'Maduras en riesgo',
    2: 'Estables',
    3: 'Nuevas abandonadas'
}
df_resultado['perfil'] = df_resultado['cluster'].map(nombres_cluster)
```

---

## Paso 8 — Guardar modelo en S3

```python
import joblib, boto3, os, tarfile

S3_MODELOS = f's3://{BUCKET}/modelos/kmeans/'

# Guardar artefactos localmente
joblib.dump(km_final, 'kmeans_model.joblib')
joblib.dump(scaler,   'scaler.joblib')
joblib.dump(encoders, 'encoders.joblib')

# Empaquetar en .tar.gz (formato requerido por SageMaker)
with tarfile.open('model.tar.gz', 'w:gz') as tar:
    tar.add('kmeans_model.joblib')
    tar.add('scaler.joblib')
    tar.add('encoders.joblib')

# Subir a S3
s3 = boto3.client('s3')
s3.upload_file('model.tar.gz', BUCKET, 'modelos/kmeans/model.tar.gz')

# Guardar también los resultados
wr.s3.to_csv(
    df=df_resultado,
    path=f'{S3_MODELOS}resultados/kmeans_perfiles.csv',
    index=False
)

print("✅ Modelo y resultados guardados en S3")
```

---

## Paso 9 — Crear script de inferencia

Crea el archivo inference.py directamente desde una celda del notebook usando la magia %%writefile. Al ejecutar la celda, Jupyter escribe el contenido como archivo real en el directorio actual:

```python
%%writefile inference.py
import joblib
import json
import numpy as np
import os

PERFILES = {
    0: {'nombre': 'Nuevas al día',        'descripcion': 'Empresas jóvenes con renovación puntual'},
    1: {'nombre': 'Maduras en riesgo',    'descripcion': 'Empresas antiguas con varios años sin renovar'},
    2: {'nombre': 'Estables',             'descripcion': 'Empresas establecidas con renovación regular'},
    3: {'nombre': 'Nuevas abandonadas',   'descripcion': 'Empresas recientes que dejaron de renovar'}
}

def model_fn(model_dir):
    model  = joblib.load(os.path.join(model_dir, 'kmeans_model.joblib'))
    scaler = joblib.load(os.path.join(model_dir, 'scaler.joblib'))
    return {'model': model, 'scaler': scaler}

def input_fn(request_body, content_type):
    if isinstance(request_body, bytes):
        request_body = request_body.decode('utf-8')

    print(f"DEBUG content_type: {content_type}")
    print(f"DEBUG request_body: {request_body}")

    body = json.loads(request_body)

    if 'instancias' in body:
        data = body['instancias']
    elif 'instances' in body:
        data = body['instances']
    else:
        data = body

    return np.array(data, dtype=float)

def predict_fn(input_data, artifacts):
    X_scaled = artifacts['scaler'].transform(input_data)
    clusters = artifacts['model'].predict(X_scaled)
    return clusters.tolist()

def output_fn(prediction, accept):
    resultado = []
    for cluster in prediction:
        perfil = PERFILES.get(cluster, {'nombre': 'Desconocido', 'descripcion': 'Sin clasificación'})
        resultado.append({
            'cluster':     cluster,
            'perfil':      perfil['nombre'],
            'descripcion': perfil['descripcion']
        })
    return json.dumps({'predicciones': resultado})
```

---


## Paso 10 — Desplegar endpoint en SageMaker

```python
import boto3, sagemaker, tarfile
from sagemaker.sklearn.model import SKLearnModel

sm_client = boto3.client('sagemaker', region_name='us-east-1')

# 1. Eliminar endpoint (si aún existe)
try:
    sm_client.delete_endpoint(EndpointName='kmeans-perfiles-empresas')
    print("🗑️ Endpoint eliminado")
except:
    print("⚠️ Endpoint ya no existe")

# 2. Eliminar endpoint config (esto es lo que faltaba)
try:
    sm_client.delete_endpoint_config(EndpointConfigName='kmeans-perfiles-empresas')
    print("🗑️ Endpoint config eliminado")
except:
    print("⚠️ Endpoint config ya no existe")

# 3. Esperar unos segundos
import time
time.sleep(10)

# 4. Redesplegar
ROLE = sagemaker.get_execution_role()
sklearn_model = SKLearnModel(
    model_data=f's3://{BUCKET}/modelos/kmeans/model_final.tar.gz',
    role=ROLE,
    entry_point='inference.py',
    framework_version='1.2-1',
    sagemaker_session=sagemaker.Session()
)

predictor = sklearn_model.deploy(
    instance_type='ml.t2.medium',
    initial_instance_count=1,
    endpoint_name='kmeans-perfiles-empresas'
)

print(f"✅ Endpoint activo: {predictor.endpoint_name}")
```

> ⏱️ El despliegue tarda entre 3 y 5 minutos. Puedes monitorear el estado en **SageMaker Studio → Deployments → Endpoints**.

---

## Paso 11 — Probar el endpoint

```python
import json

# Formato: [tipo_sociedad, org_juridica, actividad, segmento, antiguedad, anos_sin_renovar]
payload = {
    "instancias": [
        [0, 1, 5, 2, 3.5, 1.0],    # empresa nueva, renovación reciente
        [2, 0, 3, 3, 12.0, 4.2],   # empresa madura, sin renovar hace tiempo
        [1, 1, 7, 1, 6.5, 0.8]     # empresa establecida, al día
    ]
}

response = predictor.predict(json.dumps(payload))
print("Predicciones:", response)
# Salida esperada:
# {"predicciones": [
#   {"cluster": 0, "perfil": "Nuevas al día"},
#   {"cluster": 1, "perfil": "Maduras en riesgo"},
#   {"cluster": 2, "perfil": "Estables"}
# ]}
```

---

## Paso 12 — Exponer como API REST (opcional)

Para consumir el endpoint desde aplicaciones externas o dashboards:

### Arquitectura

```
Aplicación / Dashboard
        │ HTTP POST
        ▼
   API Gateway
        │ trigger
        ▼
  Lambda Function
        │ boto3
        ▼
SageMaker Endpoint
```

###  crear Función Lambda

1. Ve a AWS Console → Lambda → Create function
2. Selecciona Author from scratch
3. Configura:

    Function name: kmeans-perfiles-invoker
    Runtime: Python 3.12
    Execution role: Usa un rol GlueMedallionRol agregando la siguiente politica:
    ```
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
    ```

4. Clic en Create function
5. En el editor de código pega esto:

```python
# lambda_function.py
import boto3
import json


def lambda_handler(event, context):
    runtime = boto3.client('sagemaker-runtime')

    # Parsear el body entrante
    body = json.loads(event.get('body', '{}'))

    print(f"Lambda body: {body}")  # agregar este debug

    response = runtime.invoke_endpoint(
        EndpointName='kmeans-perfiles-empresas',
        ContentType='application/json',
        Body=json.dumps(body)   # una sola serialización
    )

    result = json.loads(response['Body'].read())
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(result)
    }
```
6. Clic en Deploy.

### B) Crear el API Gateway
 
1. Ve a **AWS Console → API Gateway → Create API**
2. Selecciona **REST API → Build**
3. Configura:
   - **API name:** `kmeans-perfiles-api`
   - **Endpoint type:** `Regional`
4. Clic en **Create API**
---
 
### C) Crear el recurso y método
 
1. En el panel izquierdo clic en **Resources → Actions → Create Resource**
   - **Resource name:** `predecir`
   - **Resource path:** `/predecir`
   - Clic en **Create Resource**
2. Con `/predecir` seleccionado → **Actions → Create Method → POST → ✓**
3. Configura la integración:
   - **Integration type:** `Lambda Function`
   - **Lambda Region:** tu región (ej. `us-east-1`)
   - **Lambda Function:** `kmeans-perfiles-invoker`
   - Clic en **Save → OK**
---
 
### D) Desplegar el API
 
1. **Actions → Deploy API**
2. Configura:
   - **Deployment stage:** `[New Stage]`
   - **Stage name:** `prod`
3. Clic en **Deploy**
4. Copia la **Invoke URL** que aparece:
   ```
   https://<api-id>.execute-api.us-east-1.amazonaws.com/prod
   ```
 
---
 
### E) Probar el API desde el notebook o terminal
 
```python
import requests
 
API_URL = 'https://<api-id>.execute-api.us-east-1.amazonaws.com/prod/predecir'
 
payload = {
    "instancias": [
        [0, 1, 5, 2, 3.5, 1.0],
        [2, 0, 3, 3, 12.0, 4.2]
    ]
}
 
response = requests.post(API_URL, json=payload)
print(response.json())
```
 
O desde terminal con curl:
 
```bash
curl -X POST https://<api-id>.execute-api.us-east-1.amazonaws.com/prod/predecir \
  -H "Content-Type: application/json" \
  -d '{"instancias": [[0, 1, 5, 2, 3.5, 1.0]]}'
```
O desde postman en raw:
```
{
    "body": "{\"instancias\": [[0, 1, 5, 2, 3.5, 1.0]]}"
}
```
---

## Costos y Limpieza

> ⚠️ Los endpoints de SageMaker **cobran por hora**, aunque no reciban tráfico.

```python
# Eliminar endpoint cuando no lo necesites (desde notebook de SageMaker)
try:
    sm_client.delete_endpoint(EndpointName='kmeans-perfiles-empresas')
    print("🗑️ Endpoint eliminado")
except:
    print("⚠️ Endpoint ya no existe")

# 2. Eliminar endpoint config (esto es lo que faltaba)
try:
    sm_client.delete_endpoint_config(EndpointConfigName='kmeans-perfiles-empresas')
    print("🗑️ Endpoint config eliminado")
except:
    print("⚠️ Endpoint config ya no existe")
```

### Estimado de costos (referencial)

| Recurso | Tipo | Costo aprox. |
|---|---|---|
| SageMaker Notebook | `ml.t3.medium` | ~$0.05/hora |
| SageMaker Endpoint | `ml.t3.medium` | ~$0.05/hora |
| Athena | Por TB escaneado | ~$5/TB |
| S3 | Almacenamiento | ~$0.023/GB/mes |

---

## Estructura del proyecto

```
proyecto-kmeans-rues/
│
├── notebooks/
│   └── kmeans_rues.ipynb          # Notebook principal (pasos 3 al 11)
│
├── src/
│   └── inference.py               # Script de inferencia para el endpoint
│
├── sql/
│   ├── dataset_ml.sql             # Consulta Athena completa
│   └── analisis_exploratorio.sql  # Consultas de soporte
│
├── outputs/
│   ├── elbow_curve.png            # Gráfica del codo
│   └── kmeans_perfiles.csv        # Resultado con clusters asignados
│
└── README.md                      # Este archivo
```

---

## Referencias

- [AWS Data Wrangler (awswrangler)](https://aws-sdk-pandas.readthedocs.io/)
- [SageMaker SKLearn Estimator](https://sagemaker.readthedocs.io/en/stable/frameworks/sklearn/sagemaker.sklearn.html)
- [scikit-learn KMeans](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html)
- [Workshop base del proyecto](https://github.com/Doc-UP-AlejandroJaimes/big-data-processing-with-aws-glue-workshop)

---

*Generado como complemento al taller de Big Data con AWS Glue — flujo extendido con Machine Learning en SageMaker.*
