import os
import time;

path_hive_labeled = '/tmp/wine_pred_' + os.environ["HADOOP_USER_NAME"] + str(int(time.time())) + '/'
path_hive_predict = '/tmp/wine_pred_hive_' + os.environ["HADOOP_USER_NAME"] + str(int(time.time())) + '/'

from pyspark.sql import SparkSession
from pyspark.sql.types import *

print("Start Spark session :")
spark = SparkSession \
  .builder \
  .appName('wine-quality-create-table') \
  .master("local[*]")\
  .getOrCreate()

# Read data
# Data does not have schema, so we declare it manually
schema = StructType([StructField("fixedAcidity", DoubleType(), True),
  StructField("volatileAcidity", DoubleType(), True),
  StructField("citricAcid", DoubleType(), True),
  StructField("residualSugar", DoubleType(), True),
  StructField("chlorides", DoubleType(), True),
  StructField("freeSulfurDioxide", LongType(), True),
  StructField("totalSulfurDioxide", LongType(), True),
  StructField("density", DoubleType(), True),
  StructField("pH", DoubleType(), True),
  StructField("sulphates", DoubleType(), True),
  StructField("Alcohol", DoubleType(), True),
  StructField("Quality", StringType(), True)
])

data_path = "file:///home/cdsw/data"
data_file = "WineNewGBTDataSet.csv"
wine_data_raw = spark.read.csv(data_path+'/'+data_file, schema=schema,sep=';')
wine_data_raw.show(3)

wine_data_raw.write.mode('overwrite').parquet(path_hive_labeled)

#Read Data and save to S3

#Create table of labeled data for training
spark.sql('''DROP TABLE IF EXISTS `default`.`{}`'''.format('wineds_ext'))
statement = '''
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`{}` (
`fixedAcidity` double ,
`volatileAcidity` double ,
`citricAcid` double ,
`residualSugar` double ,
`chlorides` double ,
`freeSulfurDioxide` bigint ,
`totalSulfurDioxide` bigint ,
`density` double ,
`pH` double ,
`sulphates` double ,
`Alcohol` double ,
`Quality` string )
STORED AS PARQUET
LOCATION '{}'
'''.format('wineds_ext', path_hive_labeled,  )
spark.sql(statement)

print("First 5 rows of labeled data - hive")
spark.sql('''SELECT * FROM wineDS_ext LIMIT 5''').show()

#Create table of unlabeled data

#Note: using same data as training...

wine_df = spark.sql(''' SELECT
`fixedAcidity`, `volatileAcidity`,
`citricAcid`, `residualSugar` ,
`chlorides` , `freeSulfurDioxide`,
`totalSulfurDioxide` ,`density` ,
`pH`, `sulphates`,`Alcohol`
FROM wineDS_ext''')

# Write in Parquet
wine_df.write.mode('overwrite').parquet(path_hive_predict)

#create external table
spark.sql('''DROP TABLE IF EXISTS `default`.`{}`'''.format('wineds_ext_nolabel'))

statement = '''
CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`{}` (
`fixedAcidity` double ,
`volatileAcidity` double ,
`citricAcid` double ,
`residualSugar` double ,
`chlorides` double ,
`freeSulfurDioxide` bigint ,
`totalSulfurDioxide` bigint ,
`density` double ,
`pH` double ,
`sulphates` double ,
`Alcohol` double )
STORED AS PARQUET
LOCATION '{}'
'''.format( 'wineds_ext_nolabel', path_hive_predict, )
spark.sql(statement)

print("First 5 rows of unlabeled data - hive")
spark.sql('''SELECT * FROM wineds_ext_nolabel LIMIT 5''').show()
