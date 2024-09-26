import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # for more sql functions
from pyspark.sql.functions import col, regexp_replace
from collections import Counter

os.environ['HADOOP_HOME'] = 'C:\hadoop'
os.environ['SPARK_HOME'] = 'C:\spark'
os.environ['JAVA_HOME'] = 'C:\Java'

# Starting a SparkSession
spark = SparkSession.builder.appName('Datasets Merge') \
                            .master("local[*]") \
                            .config("spark.hadoop.fs.defaultFS", "file:///") \
                            .getOrCreate()
spark   # Testing the app and it's working

# Loading the datasets as Spark dataframes
facebook_df = spark.read.csv('D:\Datasets/facebook_dataset.csv', header=True, inferSchema=True)
google_df = spark.read.csv('D:\Datasets/google_dataset.csv', header=True, inferSchema=True)
# We can read the website dataset using the delimiter ';' and should have 11 categories/columns
website_df = spark.read.option("delimiter", ";").csv('D:\Datasets/website_dataset.csv', header=True)

# We need to understand the structure - checking schema for each dataset
facebook_df.printSchema()
google_df.printSchema()
website_df.printSchema()

# We can show a few rows to understand and inspect better with example
facebook_df.show(truncate=False)
google_df.show(truncate=False)
website_df.show(truncate=False)

# Everything reads well so we can go to next step 

# Cleaning data and processing

# Removing the duplicates rows applying distinct
facebook_distinctDF = facebook_df.distinct()
print("Distinct count: "+str(facebook_distinctDF.count()))
facebook_distinctDF.show(truncate=False)

google_distinctDF = google_df.distinct()
print("Distinct count: "+str(google_distinctDF.count()))
google_distinctDF.show(truncate=False)

website_distinctDF = website_df.distinct()
print("Distinct count: "+str(website_distinctDF.count()))
website_distinctDF.show(truncate=False)

# Remove row if all values are NULL
facebook_df.na.drop("all")
google_df.na.drop("all")
website_df.na.drop("all")

# Removing non-readable characters
readable_regex = "[^\\x20-\\x7E]"
cleaned_facebook_df = facebook_df.select(
    *[regexp_replace(col(c), readable_regex, "").alias(c) for c in facebook_df.columns]
)
cleaned_google_df = google_df.select(
    *[regexp_replace(col(c), readable_regex, "").alias(c) for c in google_df.columns]
)
cleaned_website_df = website_df.select(
    *[regexp_replace(col(c), readable_regex, "").alias(c) for c in website_df.columns]
)

# Clean standardized phone numbers, remove non-numeric characters
facebook_df = facebook_df.withColumn("phone", regexp_replace("phone", r"\D", ""))
google_df = google_df.withColumn("phone", regexp_replace("phone", r"\D", ""))
website_df = website_df.withColumn("phone", regexp_replace("phone", r"\D", ""))
# facebook_df.show(truncate=False)
# google_df.show(truncate=False)   Just checking - and it's working 
website_df.show(truncate=False)


# Join and merge datasets

# We can try full outer joins to combine all the data first
google_df = google_df.withColumn("categories", col("category")).drop("category")

#First join will be on the columns that interests us the most
first_join = facebook_df.join(google_df, on=["categories", "address", "country_name", "region_name", "phone", "name"], how="full_outer")
first_join.show(25, truncate=False)

final_join_df = first_join.join(website_df, on=["phone"], how="outer")
final_join_df.show(25, truncate=False)

final_join_distinctDF = final_join_df.distinct()
print("Distinct count: "+str(final_join_distinctDF.count()))
final_join_distinctDF.show(50,truncate=False)

# Removing special chars in address column

endgame_joinDF = final_join_distinctDF.withColumn("address", regexp_replace("address", "[^a-zA-Z0-9 ]+", ""))
endgame_joinDF.show(50, truncate=False)

# We have some name duplicates in the header after final join and can't write new CSV Data
# The names that interests us are looking good, so the other duplicates we can rename them to avoid further errors. 

df_cols = endgame_joinDF.columns

# Dictionary to count occurrences of each column name
count_map = {}
new_cols = []

for col in df_cols:
    # Check if the column has been seen before
    if col in count_map:
        count_map[col] += 1
        # Create a new name using the count
        new_col_name = f"{col}_{count_map[col]}"
    else:
        count_map[col] = 1
        # Keep the original name if it's the first occurrence
        new_col_name = col
    
    new_cols.append(new_col_name)

# Rename the columns in the DataFrame
endgame_joinDF = endgame_joinDF.toDF(*new_cols)
endgame_joinDF.show(100, truncate=False)

# All looks good in the columns that interests us

# output_path = "C:/Users/bogda_000/Desktop/"
# endgame_joinDF.write.save("C:/Users/bogda_000/Desktop/", "csv", "append")
# endgame_joinDF.coalesce(1).write.csv(output_path, header=True, mode='overwrite', delimiter=";")

# NOTE FOR THE REVIEWER: 

# I didn't manage to write the final dataset into a new CSV, I got a lot of errors Py4JJavaError
# and I've tried one whole day a lot of solutions. Obviously It didn't work unfortunately. 
# Maybe my PC and setup configuration is the problem or maybe I did make a mistake somewhere and just can't find it.
# Have a nice day! 



