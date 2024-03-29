import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col,monotonically_increasing_id,rand,regexp_extract
from config import credentials_path, jar_file_path, gcs_bucket_path, output_path
from utils import order_columns
# Spark configuration
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", jar_file_path) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
    .set("spark.hadoop.google.cloud.project.id", "gothic-sylph-387906")

# Create Spark Context
sc = SparkContext(conf=conf)

# Hadoop configuration for GCS access
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_path)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Create Spark Session
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# Load data from GCS bucket
print(f'Loading Data from GCS Bucket path {gcs_bucket_path}')
df_raw = spark.read.parquet(gcs_bucket_path + 'raw_streaming/*')
print('Done')

# df_raw.show()

# Function to extract specific columns from DataFrame
def extract_columns(df, columns_to_extract):
    extracted_cols = [col("data").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

# Function to create dimension tables
def create_dimension_tables(df, extract_func, columns_to_extract, table_name):
    order_data = extract_func(df, columns_to_extract)
    # Uncomment the line below if you want to create a temporary view for SQL queries
    # dimension_df.createOrReplaceTempView(table_name)
    return order_data

order_data = create_dimension_tables(df_raw, extract_columns, order_columns, "customer_dimension")
order_data = order_data.withColumn('customer_id',(rand()*287+1).cast('int'))
# order_data.show()


customer_data=spark.read.options(header=True).csv("streaming_pipeline/data/onlinedeliverydata.csv")

customer_data = customer_data.dropDuplicates().\
                                withColumn('customer_id',monotonically_increasing_id()+1).\
                                withColumnRenamed('Medium (P1)','Medium_P1').\
                                withColumnRenamed('Medium (P2)','Medium_P2').\
                                withColumnRenamed('Meal(P1)','Meal_P1').\
                                withColumnRenamed('Meal(P2)','Meal_P2').\
                                withColumnRenamed('Perference(P1)','Perference_P1').\
                                withColumnRenamed('Perference(P2)','Perference_P2')
customer_data.printSchema()

# order_data = spark.read.options(header=True).csv('streaming_pipeline/data/train.csv')
# order_data = order_data.withColumn('customer_id', (rand()*287 +1).cast('int'))
# order_data.printSchema()

dim_delivery_person = order_data.select('Delivery_person_ID','Delivery_person_Age','Delivery_person_Ratings',
                                        'Vehicle_condition','Type_of_vehicle')

# dim_delivery_person.show()

# Select relevant columns and alias them
dim_location = order_data.select(
    'Restaurant_latitude',
    'Restaurant_longitude',
    col('Delivery_location_latitude').alias('Delivery_latitude'),
    col('Delivery_location_longitude').alias('Delivery_longitude'),
    'City',col('customer_id').alias('order_customer_id')
).join(
    customer_data.select(
        col('latitude').alias('customer_latitude'),
        col('longitude').alias('customer_longitude'),
        'Pin code',col('customer_id')
    ),
    on=col('order_customer_id') == col('customer_id'),  # Join condition based on customer_id
    how='inner'  # Inner join
)
# dim_location.show()
dim_time = order_data.select('Order_Date',col('Time_Orderd').alias('Time_Ordered'),'Time_Order_picked','Weatherconditions','Road_traffic_density','Festival')
# dim_time.show()

fact_order = order_data.withColumn("Time_taken", regexp_extract("Time_taken(min)", r"(\d+)", 1))\
                        .select(col('ID').alias('order_id'),'customer_id','Delivery_person_ID','Order_Date',col('Time_taken(min)').alias('Time_taken'),'multiple_deliveries',
                                    'Type_of_order').\
                        withColumn('order_amount',(rand()*1000+1).cast('int'))
# fact_order.show()


# # Dictionary to hold all dimension tables
dataframes = {
    "customer_dimension": customer_data,
    "delivery_person_dimension": dim_delivery_person,
    "location_dimension": dim_location,
    "fact_order": fact_order,
    "time_dimension": dim_time
}

# Function to write dimension tables to GCS
def write_to_gcs(dataframes, output_path):
    print('Starting to Export Raw Streaming data to GCS...')
    for name, dataframe in dataframes.items():
        dataframe.write.mode("overwrite").option("header", "true").option("compression", "none").parquet(output_path + name)
        print(f"Exported Dataframe {name} to GCS.")

# Write dimension tables to GCS
write_to_gcs(dataframes, output_path)
