#Python Core Api Solution
pyspark --master yarn --conf spark.ui.port=12643


CrimeData = sc.textFile("/user/pkandi/Crime")

Header = CrimeData.first()
CrimeDataWithoutHeader = CrimeData.filter(lambda line: line != Header)

import re
def getResidence(crime):
	crimesAtResidence = re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", crime)[7] == "RESIDENCE"
	return crimesAtResidence

CrimeAtResidence =  CrimeDataWithoutHeader.filter(lambda crime: getResidence(crime))	
	
def typeOfCrime(crime):
	type = re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", crime)[5]
	return type


CrimeTypes = CrimeAtResidence.map(lambda crime: (typeOfCrime(crime),1))

TotalNoOfCrimes = CrimeTypes.reduceByKey(lambda total,value: total + value).map(lambda crime: (crime[1],crime[0]))

TopThreeCrimeTypes = TotalNoOfCrimes.sortByKey(False).take(3)

CrimeCountForResidence = TopThreeCrimeTypes.map(lambda crime: (crime[1],crime[0])).toDF("crime_type", "crime_count")

 CrimeCountForResidence.write.json("Location")






