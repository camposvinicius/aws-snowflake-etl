from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

DELIVERY_PATH = 's3://data-deliveryzone/data/'
PROCESSING_PATH = 's3://data-processingzone/data/'

def to_delivery(path_source,path_target):
    
    df = (
        spark.read.format("parquet")
        .load(path_source)
    )
            
    df.cache()        

    (
        df.write
        .partitionBy("COUNTRY")
        .format("parquet")
        .mode("overwrite")
        .save(path_target)
    )
    
    
if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    to_delivery(PROCESSING_PATH, DELIVERY_PATH) 