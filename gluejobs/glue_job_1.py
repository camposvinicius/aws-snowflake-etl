from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.types import IntegerType,DecimalType
from pyspark.sql.functions import to_date, coalesce

RAW_PATH = 's3://data-rawzone/data/2m_Sales_Records.csv'
PROCESSING_PATH = 's3://data-processingzone/data/'

def convert_to_parquet(path_source,path_target):
    
    df = (
        spark.read.format("csv")
        .option("sep", ",")
        .option("header", True)
        .load(path_source)
    )    

    for name in df.schema.names:
        df = df.withColumnRenamed(name, name.replace(' ', '_').upper())

    for column in df.columns:
        if column in ('ORDER_ID','UNITS_SOLD'):
            df = df.withColumn(column, df[f'{column}'].cast(IntegerType()))
        elif column in ('UNIT_PRICE','UNIT_COST','TOTAL_REVENUE','TOTAL_COST','TOTAL_PROFIT'):
            df = df.withColumn(column, df[f'{column}'].cast(DecimalType(10,2)))
        elif column in ('ORDER_DATE','SHIP_DATE'):       
            df = df.withColumn(column, coalesce(to_date(f'{column}', 'M/d/yyyy'), to_date(f'{column}', 'MM/dd/yyyy')))
            
    df.cache()        

    (
        df.coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(path_target)
    )
    
    
if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    convert_to_parquet(RAW_PATH, PROCESSING_PATH) 