Details - Duration 40 minutes
Choose language of your choice Python or Scala
Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,”
Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip



--Spark-Shell Code
pyspark --master yarn --conf spark.ui.port=12643

CrimeData = sc.textFile("/user/pkandi/Crime")
Header = CrimeData.first()
CrimeDataWithoutHeader = CrimeData.filter(lambda line: line != Header)

from pyspark.sql import Row

CrimeDataWithDateAndTypeDF = CrimeDataWithoutHeader.map(lambda crime: Row(crime_date = (crime.split(",")[2]), crime_type = (crime.split(",")[5]))).toDF()

CrimeDataWithDateAndTypeDF.registerTempTable("crime_data")

crimeCountPerMonthPerTypeDF = sqlContext.sql("select cast(concat(substr(crime_date, 7, 4), substr(crime_date, 0, 2)) as int) crime_month, count(1) crime_count_per_month_per_type, crime_type from crime_data group by cast(concat(substr(crime_date, 7, 4), substr(crime_date, 0, 2)) as int), crime_type order by crime_month, crime_count_per_month_per_type desc")
  
crimeCountPerMonthPerTypeDF.show(100)  

crimeCountPerMonthPerTypeDF.write.options(delimiter="\t", codec="org.apache.hadoop.io.compress.GzipCodec").save('/user/pkandi/CrimeResultsSQLGZIP')

 data.save("/user/pkandi/text").options(delimiter="\t")
 
 data.write.text("/user/pkandi/text").options(delimiter="\t")
 
 check = data.rdd.map(lambda rec: rec.join("\t"))
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 codec="org.apache.hadoop.io.compress.GzipCodec"
 
 
 hadoop fs -cat /user/pkandi/CrimeResultsSQL/part-r-00000-bcf2fbb6-1c7e-4eb1-b146-168f27526b68.gz.parquet  | gzip -d | hadoop fs -put - /user/pkandi/CrimeResultsUnzipped/part0000.txt
 
 /user/pkandi/CrimeResultsSQLGZIP/part-r-00000-2c460e13-2a50-48d1-8f1d-6856f428e51a.gz.parquet
 
 data = sqlContext.read.parquet("/user/pkandi/CrimeResultsSQLGZIP/part-r-00000-2c460e13-2a50-48d1-8f1d-6856f428e51a.gz.parquet")
 
 data.show()
 
