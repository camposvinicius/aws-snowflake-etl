# ETL for Business Team

### This is a data pipeline built with the purpose of serving a business team.

First, the pipeline itself is a sequence of steps and relatively simple, as in the image below, where we use the **Step Function** tool as our main orchestrator.

<br />

![stepfunction](screenshots/stepfunction.png)

<br />

All resource creation was done via **Cloudformation** and basically we have two stacks, one called _dataengineer-requirements_ where we create our codes bucket and our docker images repository and then the rest of all the necessary infrastructure for the pipeline called _dataengineer-stack_.

<br />

![CloudFormation](screenshots/cloudformation.png)

<br />

We've also created two **CICD pipelines**, one for creating resources whenever there's a PUSH on the master branch, and another for destroying resources that can be triggered manually.

<br />

![cicd-construct](screenshots/cicd-construct.png)

<br />

This set of buckets creates our datalake and the data goes through the complete ETL process involving all the main layers, here we call _data-rawzone, data-processingzone and data-deliveryzone_.

<br />

![Datalake](screenshots/buckets.png)

<br />

Now let's get into the [lambda function code](https://github.com/camposvinicius/datachallenge_public_repo/blob/main/lambda_extract/lambda_function.py) and understand what it is all about.

<br />

```python
import requests, io, tempfile, os, boto3
from zipfile import ZipFile

file_name = '2m-Sales-Records.zip'
bucket = "data-rawzone"
url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'

def lambda_handler(event, context):

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    s3 = boto3.resource('s3')

    with tempfile.TemporaryDirectory() as temp_path:
        temp_dir = os.path.join(temp_path, 'temp')
        with open(temp_dir, 'wb') as f:
            req = requests.get(url, headers=headers)  
            f.write(req.content)
        
        s3.Bucket(bucket).upload_file(temp_dir, file_name)
    
        zip_obj = s3.Object(bucket_name=bucket, key=file_name)
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        
        z = ZipFile(buffer)
        for filename in z.namelist():
            s3.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=bucket,
                Key='data/' + f'{filename}'
            )

    for file in s3.Bucket(bucket).objects.all():

        ## Removing the whitespaces in the filename

        if str(file).__contains__('data/'):
            OLD_NAME = file.key
            NEW = file.key.replace(" ", "_")

            try:
                s3.Object(bucket, NEW).copy_from(CopySource=f'{bucket}/{OLD_NAME}')
                s3.Object(bucket, OLD_NAME).delete()
            except:
                pass
            
        ## Moving original file to another prefix    

        elif str(file).__contains__('.zip'):
            OLD_PATH = file.key
            NEW_PREFIX = 'old-data/'+OLD_PATH

            try:
                s3.Object(bucket, NEW_PREFIX).copy_from(CopySource=f'{bucket}/{OLD_PATH}')
                s3.Object(bucket, OLD_PATH).delete()
            except:
                pass
```
<br />

Basically, we have a **containerized lambda function** that is making a request and writing the content, which is a zip file, inside a temporary directory to later upload to s3, unzip and extract the csv file that is inside it, replace the blank spaces in the filename by underscores and move the zip file to another prefix within the same bucket.

This is the result, where in data we have the renamed csv file and old-data we have the zip file.

<br />

![raw](screenshots/raw.png)

<br />

After that, we have a **glue workflow using spark jobs and crawler** that will basically do:

Read the csv file from the raw zone &#8594; 
  Save to parquet in just a single partition in the processing zone &#8594;
    Save to parquet and partitioned by COUNTRY in the delivery zone &#8594; 
      Trigger the glue crawler to make the table available in Athena.

<br />

![glueworkflow](screenshots/glue-workflow.png)

You can check what the **codes** are doing in the [code1](https://github.com/camposvinicius/datachallenge_public_repo/blob/main/gluejobs/glue_job_1.py) and [code2](https://github.com/camposvinicius/datachallenge_public_repo/blob/main/gluejobs/glue_job_2.py).

The results:

<br />

![processingzone](screenshots/processing_to_parquet.png)

<br />

![deliverzone](screenshots/deliveryzone.png)

<br />

After the **crawler** runs, here is our final table with the data schema already enriched, since all the fields in our original schema were _strings_, which you can check how it was done in our glue spark job 1.

<br />

![table](screenshots/query_test.png)

<br />

```sql
WITH metrics as (
    select 
        country,
        order_date,
        total_revenue,
        row_number() over (partition by country order by total_revenue desc) as R
    from data_deliveryzone where country in ('Canada', 'United States of America')
)
SELECT
    country,
    order_date,
    total_revenue
FROM
    metrics
where R <=2    
order by 3 desc;
```

<br />


Where we use this simple query as an example to get the two biggest revenues from Canada and USA, and your order dates.

And finally we have the destruction of resources via our CICD pipeline that we mentioned at the beginning.

<br />

![cicd-destruction](screenshots/cicd-destroy.png)

<br />

![cloudformationdestruction](screenshots/delete-cloudformation.png)

You can understand the code to construct [here](https://github.com/camposvinicius/datachallenge_public_repo/blob/main/.github/workflows/deploy.yml) and 
to destroy [here](https://github.com/camposvinicius/datachallenge_public_repo/blob/main/.github/workflows/destroy.yml).

## -------------------------------------------------- EXTRA BONUS --------------------------------------------------

Let's imagine that you need to make the data available not only in **Athena** but also in **![Snowflake](https://miro.medium.com/max/1088/1*L02Ojuxa39jZefsd2LkbXw.png)**

Well, there are many ways to do this, let's show you one of them. First we can create a _snowflake integration_ with our dalalake using this structure:

```sql
create or replace storage integration datadelivery
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/RoleToAccessS3DataDeliveryZone'
storage_allowed_locations = (
    's3://data-deliveryzone/data/'
);
```

<br />

![sf-integration](screenshots/integration_snowflake.png)

<br />

_Remembering that you need to create a Role that will have a policy that gives you the access to your s3 bucket that you want._

You can follow this [step by step](https://docs.snowflake.com/en/user-guide/data-load-s3-config-aws-iam-role) to understand how to configure the integration and role.

After that you will create a STAGE using the created integration, which is basically where the files will be inside the snowflake.

<br />

```sql
create or replace stage DELIVERYZONE.DELIVERYZONE.STAGE_DELIVERYZONE
url='s3://data-deliveryzone/data/'
storage_integration = datadelivery;
```

<br />

![sf-stage](screenshots/stage.png)

<br />

You will also need to create the file format that snowflake will work in and ingest with your file, in our case is parquet.

```sql
CREATE OR REPLACE FILE FORMAT DELIVERYZONE.DELIVERYZONE.PARQUET
TYPE = PARQUET
SNAPPY_COMPRESSION = TRUE
BINARY_AS_TEXT = TRUE
TRIM_SPACE = FALSE
NULL_IF = ('NULL', 'NUL', '');
```

<br />

![sf-fileformat](screenshots/fileformat.png)

<br />

Now basically you can make the table available directly integrated to your datalake, via external table. First map the schema of your table and you can follow the idea below, in this case, we have a partitioned table and to help you map your partition you can do a query like this to understand your partitioned table and metadata:

<br />

```sql
select 
    metadata$filename, 
    (split_part(split_part(metadata$filename, '/', 2),'=', 2)::TEXT)
FROM @DELIVERYZONE.DELIVERYZONE.STAGE_DELIVERYZONE (file_format => 'DELIVERYZONE.DELIVERYZONE.PARQUET') t;
```

<br />

![sf-metadatafilename](screenshots/metadata_filename.png)

<br />

```sql
create or replace external table DELIVERYZONE.DELIVERYZONE.TABLE_DELIVERYZONE (
  REGION string AS (value:REGION::string), 
  ITEM_TYPE string AS (value:ITEM_TYPE::string), 
  SALES_CHANNEL string AS (value:SALES_CHANNEL::string), 
  ORDER_PRIORITY string AS (value:ORDER_PRIORITY::string), 
  ORDER_DATE date AS (value:ORDER_DATE::date), 
  ORDER_ID int AS (value:ORDER_ID::int), 
  SHIP_DATE date AS (value:SHIP_DATE::date), 
  UNITS_SOLD int AS (value:UNITS_SOLD::int), 
  UNIT_PRICE decimal AS (value:UNIT_PRICE::decimal(10,2)), 
  UNIT_COST decimal AS (value:UNIT_COST::decimal(10,2)), 
  TOTAL_REVENUE decimal AS (value:TOTAL_REVENUE::decimal(10,2)), 
  TOTAL_COST decimal AS (value:TOTAL_COST::decimal(10,2)), 
  TOTAL_PROFIT decimal AS (value:TOTAL_PROFIT::decimal(10,2)), 
  COUNTRY STRING AS (split_part(split_part(metadata$filename, '/', 2),'=', 2)::TEXT)
    ) 
  partition by (COUNTRY)
  location = @DELIVERYZONE.DELIVERYZONE.STAGE_DELIVERYZONE
  auto_refresh = true 
  file_format = (FORMAT_NAME='DELIVERYZONE.DELIVERYZONE.PARQUET'); 
```

<br />

And here is our external table created.

<br />

![sf-external_table_creation](screenshots/external_table_creation.png)

<br />

<br />

![sf-external_table_preview](screenshots/external_table_preview.png)

<br />
