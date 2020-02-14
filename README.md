# Scala-project
Spark Use Case – Olympics Data Analysis
As one of the spark use case, we will discuss the analysis of Olympics dataset using Apache Spark in Scala,

Olympics data set is a publicly available data. Using this dataset, we will evaluate some problem statements such as, finding the number of medals won by each country in swimming, finding the number of medals won by India etc.

Data Set Description

The data set consists of the following fields:

Athlete: Name of the athlete

Age: Age of the athlete

Country: The name of the country participating in Olympics

Year: The year in which Olympics is conducted

Closing Date: Closing date of Olympics

Sport: Sports name

Gold Medals: No. of gold medals

Silver Medals: No. of silver medals

Bronze Medals: No. of bronze medals

Total Medals: Total no. of medals

Dataset Link : 
https://drive.google.com/drive/folders/0ByJLBTmJojjzVGNsWmpUUUxTZDA

Problem Statement 1

Find the total number of medals won by each country in swimming.

Source code

val textFile = sc.textFile("hdfs://localhost:9000/olympix_data.csv")
val counts = textFile.filter { x => {if(x.toString().split("\t").length >= 10) true else false} }.map(line=>{line.toString().split("\t")})
val fil = counts.filter(x=>{if(x(5).equalsIgnoreCase("swimming")&&(x(9).matches(("\\d+")))) true else false })
val pairs: RDD[(String, Int)] = fil.map(x => (x(2),x(9).toInt))
val cnt = pairs.reduceByKey(_ + _).collect()
