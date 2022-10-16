import os, re
import configparser
import boto3
import glob 
import pandas as pd
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType

date_format = "YYYY-MM-DD"

def create_spark_session():
    
    """
    This function creates a session with Spark, the entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").\
    enableHiveSupport().getOrCreate()
    print(spark.version)
    return spark


def etl_immigration_data(spark, output_path):
    
    """
    Reads the immigration dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated 
    by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
        Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
    """
    
    i94_file_paths = glob.glob("../../data/18-83510-I94-Data-2016/*.sas7bdat")

    
    df_immigration=spark.read.format('com.github.saurfang.sas.spark').\
    load(i94_file_paths[0],inferLong=True, forceLowercaseNames=True)
            
    i94_file_paths.pop(0)
    x = 0
    for file in i94_file_paths:
        tempDF = spark.read.format('com.github.saurfang.sas.spark').\
            load(file,inferLong=True, forceLowercaseNames=True)
        if(len(tempDF.columns)==34):
            tempDF = tempDF.drop('delete_days','delete_mexl','delete_dup','delete_visa','delete_recdup','dtadfile')
        df_immigration.union(tempDF)
        x = x+1
        
    print("number of files appedned is = "+ str(x))
    df_immigration.printSchema()
    df_immigration.show()
    
    date_cols = ['arrdate', 'depdate'] 
    
    # Convert SAS date to a meaningful string date in the format of YYYY-MM-DD
    df_immigration = convert_sas_date(df_immigration, date_cols)
    
    # select the attributes that will be used and droping the duplicates.

    immigration_table = df_immigration.select('cicid','admnum','i94cit','i94port','arrdate', 'depdate').dropDuplicates(subset=['admnum'])
    
    immigration_table.show()
    immigration_table.printSchema()
    
    

    # select the attributes that will be used and droping the duplicates.

    person_table = df_immigration.select('admnum','i94res','i94port','i94mode',\
                                        'i94cit','i94mon','arrdate','i94addr',\
                                        'depdate','i94bir','i94visa','occup',\
                                        'biryear','gender','airline',\
                                        'fltno').dropDuplicates(subset=['admnum'])
    person_table.show()
    person_table.printSchema()
    
#     Save the processed immigration dataset to the output_path
    immigration_table.write.parquet(output_path+"immigration",mode="overwrite")
    person_table.write.parquet(output_path+"person",mode="overwrite")
        
def etl_airport_data(spark, input_path, output_path):
    """
    Reads the airport dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated 
    by the    parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save airport output files.
    """
    # Loads the airport dataframe using Spark
    
    df_airport = spark.read.csv(input_path)
    
    # renaming the columns to a representative name.
    df_airport = df_airport.withColumnRenamed("_c0","ID")\
                            .withColumnRenamed("_c1","type")\
                            .withColumnRenamed("_c2","name")\
                            .withColumnRenamed("_c3","elevation_ft")\
                            .withColumnRenamed("_c4","continent")\
                            .withColumnRenamed("_c5","iso_country")\
                            .withColumnRenamed("_c6","iso_region")\
                            .withColumnRenamed("_c7","municipality")\
                            .withColumnRenamed("_c8","gps_code")\
                            .withColumnRenamed("_c9","iata_code")\
                            .withColumnRenamed("_c10","local_code")\
                            .withColumnRenamed("_c11","coordinates")

    # removing the first row because it's the header
    
    df_airport = df_airport.where(df_airport.ID != 'ident')
    
    df_airport.show()
    
    # select the attributes that will be used and droping the duplicates.
    
    airport_table = df_airport.selectExpr('ID','type','name','iso_country','municipality','iata_code','local_code')\
    .dropDuplicates(subset=['ID'])
    
    airport_table.show()
    
    airport_table.printSchema()
    
#     Save the airport dataset to the output_path
    airport_table.write.parquet(output_path + "airport",mode="overwrite")

def etl_demographics_data(spark, input_path, output_path):

    """
    Reads the demographics dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated
    by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save demographics output files.
    """
    
    # Loads the demographics dataframe using Spark
    demographics = spark.read.csv(input_path, sep =';')
    
    # renaming the columns to a representative name.
    demographics = demographics.withColumnRenamed("_c0","city")\
                            .withColumnRenamed("_c1","state")\
                            .withColumnRenamed("_c2","Median_age")\
                            .withColumnRenamed("_c3","male_population")\
                            .withColumnRenamed("_c4","female_population")\
                            .withColumnRenamed("_c5","total_population")\
                            .withColumnRenamed("_c6","Number_of_ventreans")\
                            .withColumnRenamed("_c7","foreign_born")\
                            .withColumnRenamed("_c8","Average_Household_Size")\
                            .withColumnRenamed("_c9","state_code")\
                            .withColumnRenamed("_c10","race")\
                            .withColumnRenamed("_c11","Count")
    
    # changing the data types to the appropriate data types.
    
    demographics = demographics.withColumn("city",col("city").cast(StringType()))\
                            .withColumn("state",col("state").cast(StringType()))\
                            .withColumn("Median_age",col("Median_age").cast(DoubleType()))\
                            .withColumn("male_population",col("male_population").cast(IntegerType()))\
                            .withColumn("female_population",col("female_population").cast(IntegerType()))\
                            .withColumn("total_population",col("total_population").cast(IntegerType()))\
                            .withColumn("Number_of_ventreans",col("Number_of_ventreans").cast(IntegerType()))\
                            .withColumn("foreign_born",col("foreign_born").cast(IntegerType()))\
                            .withColumn("Average_Household_Size",col("Average_Household_Size").cast(DoubleType()))\
                            .withColumn("state_code",col("state_code").cast(StringType()))\
                            .withColumn("race",col("race").cast(StringType()))\
                            .withColumn("Count",col("Count").cast(IntegerType()))
    
    demographics = demographics.where(demographics.city != 'City')
    demographics.show()
    demographics.printSchema()
    # Save the demographics dataset to the output_path
    if output_path is not None:
        demographics.write.parquet(output_path+"demographics",mode="overwrite") 

def convert_sas_date(df, cols):
    """
    Convert dates in the SAS datatype to a date in a string format YYYY-MM-DD
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        cols (:obj:`list`): List of columns in the SAS date format to be convert
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

def change_field_value_condition(df, change_list):
    '''
    Helper function used to rename column values based on condition.
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        change_list (:obj: `list`): List of tuples in the format (field, old value, new value)
    '''
    for field, old, new in change_list:
        df = df.withColumn(field, when(df[field] == old, new).otherwise(df[field]))
    return df

def rename_columns(df, mapping):
    '''
    Rename the columns of the dataset based in the mapping dictionary
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        mapping (:obj: `dict`): Mapping dictionary in the format {old_name: new_name}
    '''
    df = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    return df

def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days
    
# User defined functions using Spark udf wrapper function to convert SAS dates into string dates in the format YYYY-MM-DD, to capitalize the first letters of the string and to calculate the difference between two dates in days.
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
capitalize_udf = udf(lambda x: x if x is None else x.title())
date_diff_udf = udf(date_diff)
# "%Y-%m-%d"

if __name__ == "__main__" :

    spark = create_spark_session()
    
    output_data = "s3a://stagings33/"
    input_path_US_cities ="s3a://source-data2/inputs/us-cities-demographics.csv"
    input_path_airports = 's3a://source-data2/inputs/airport-codes_csv.csv'
    
    # Perform ETL process for the Immigration dataset generating immigration and date tables and save them in the S3 bucket indicated in the output_path parameters.
    etl_immigration_data(spark, output_data)
    etl_demographics_data(spark, input_path = input_path_US_cities, output_path = output_data)
    etl_airport_data(spark, input_path = input_path_airports, output_path = output_data)
    
    spark.stop()
