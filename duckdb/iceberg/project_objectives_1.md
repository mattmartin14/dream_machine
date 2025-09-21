### Overview
The goal of this project is to connect a Google BigLake catalog to Duckdb.

As of Duckdb version 1.4, the iceberg extension supports writes and I wanted to see if this was possible with a biglake connection.


### Some Important information
Below are my gcp important items you should be aware of:

project-id: test-matt-219200
biglake catalog: biglakecat
biglake database: db1
gcs bucket: data-mattm-test-sbx
GCS folder where i want my iceberg warehouse: ice_ice-baby

### Establishing the BigLake catalog and database
I had tried several commands such as `gcloud biglake` and `gcloud bigquery biglake` but those did not work. It almost appears as if the biglake service is still very "beta". I ended up using the curl commands below to actually establish the biglake catalog and the biglake database:

```bash
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://biglake.googleapis.com/v1/projects/test-matt-219200/locations/US/catalogs?catalogId=biglakecat" \
  -d '{}'

curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://biglake.googleapis.com/v1/projects/test-matt-219200/locations/US/catalogs/biglakecat/databases?databaseId=db1" \
  -d '{
        "type": "HIVE",
        "hiveOptions": {
          "locationUri": "gs://data-mattm-test-sbx/ice_ice-baby/"     
        }
      }'
```
### Authentication
I normally authenticate to google using my default application credentials. In the past with duckdb, i'd simply intall fsspec and gcsfs and then run this command to register GCS:

```python
import duckdb
cn = duckdb.connect()
cn.register_filesystem(filesystem('gcs'))
```

From there, i could easily run select from read_parquet () and put a GS path in there.

However, with biglake, it appears you have to auth via oauth2 and grab a token. to get a token, i have a helper py file called [gcs_auth.py](./gcs_auth.py).


## Some Other nuances
I noticed for biglake, the google bigquery docs indicated I needed to establish an external vertex AI connection, which i did called "test_cn_matt"


## API Permission
I've enabled the biglake API; i also have google storage admin and biglake admin permissions attached to my default application role.

### Attempting to Connect to Biglake
In duckdb, I've tried numerous attach statements such as:

```python
cn = duckdb.connect()
cn.execute("INSTALL iceberg; INSTALL httpfs;")
cn.execute("LOAD iceberg;")
endpoint = "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"

import gcs_auth
gcs = gcs_auth.BigLakeAuthHelper(cn)
token = gcs.get_token()

bucket = os.getenv("GCS_BUCKET")

cn.execute(f"""
    ATTACH 'biglake' AS biglake (
        TYPE ICEBERG,
        ENDPOINT 'https://biglake.googleapis.com/iceberg/v1beta/projects/test-matt-219200/locations/US/catalogs/biglakecat',
        AUTHORIZATION_TYPE OAUTH2,
        TOKEN '{token}'
    );
""")

```

But this fails with this error message:

```bash
Request to 'https://biglake.googleapis.com/iceberg/v1beta/projects/test-matt-219200/locations/us/catalogs/biglakecat/v1/config?warehouse=biglake' returned a non-200 status code (NotFound_404), with reason: Not Found
```

From that error, it appears duckdb was tacking the warehouse arg to the end of the endpoint URI. So i tried that as well here:

```python
cn.execute(f"""
    ATTACH 'gs://{bucket}/ice_ice_baby' AS biglake (
        TYPE ICEBERG,
        ENDPOINT 'https://biglake.googleapis.com/iceberg/v1beta/projects/test-matt-219200/locations/US/catalogs/biglakecat',
        AUTHORIZATION_TYPE OAUTH2,
        TOKEN '{token}'
    );
""")
```

and received this error: 

```bash
Request to 'https://biglake.googleapis.com/iceberg/v1beta/projects/test-matt-219200/locations/us/catalogs/biglakecat/v1/config?warehouse=gs%3A%2F%2Fdata-mattm-test-sbx%2Fice_ice_baby' returned a non-200 status code (NotFound_404), with reason: Not Found
```

So, i'm not sure if i'm just doing something wrong, if the BigLake API is just terrible or what. My suspicion though is I just have something wrong here, becuase I thought the entire point of the Iceberg Rest catalog was it standardizes how we create iceberg tables/etc.

### Some Helpful links
This link shows GCP using spark to talk to the iceberg rest endpoing: https://cloud.google.com/bigquery/docs/blms-rest-catalog#required_roles
duckdb-iceberg git repo for the iceberg extension that enables this stuff: https://github.com/duckdb/duckdb-iceberg
duckdb repo: https://github.com/duckdb/duckdb
Biglake API: https://cloud.google.com/bigquery/docs/reference/biglake/rest


### Goal
I'd like to get a working version of duckdb using the attach statement to talk to my biglake iceberg rest catalog. Please make the necessary folders and test folders to get this done.
