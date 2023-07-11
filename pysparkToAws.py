from pyspark import SparkConf,SparkContext 
from pyspark.sql import SparkSession
import os

# adding the packages required to get data from s3.
os.environ["PYSPARK_SUBMIT_ARGS"]="--packages com.amazonaws:aws-java-sdk-s3:1.12.196.org.apache.hadoop-aws:3.3.1 pyspark-shell"

# creating spark configuration.
conf=SparkConf().setAppName('S3toSpark')

sc=SparkContext(conf=conf)

# create our spark session.
spark=SparkSession(sc).builder.appName("S3App").getOrCreate()

# configure the setting to read from the S3 bucket.
hadoopConf=sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key','AKIATCSIL7EL6FPX2T4W')
hadoopConf.set('fs.s3a.secret.key','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')
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

df=spark.read.option("header",True).csv("s3a://bucket-pyspark/StudentData.csv")
df.show()

df.write.option("header",True).csv("s3a://bucket-pyspark/StudentData1.csv",mode="overwrite")