import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object MainFile {
  /* This is my first java program.
  * This will print 'Hello World' as the output
  */
  def main(args: Array[String]) {

    // logs of the app is disabled
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf().setAppName("Second").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile("C:\\Users\\Amith\\IdeaProjects\\Olympics data maven\\olympix_data.csv")
    numberOfMedalsOfCountries(textFile)
    numberofmedalsthatIndiawonyearwise(textFile)
    totalnumberofmedalswonbyeachcountry(textFile)

  }
//Problem 01
  def numberOfMedalsOfCountries(data:RDD[String]):Unit={
    val counts = data.filter { x => {if(x.toString().split("\t").length >= 10) true else false} }.map(line=>{line.toString().split("\t")})

    val fil = counts.filter(x=>{if(x(5).equalsIgnoreCase("swimming")&&(x(9).matches(("\\d+")))) true else false })
    val pairs: RDD[(String, Int)] = fil.map(x => (x(2),x(9).toInt))
    val cnt = pairs.reduceByKey(_ + _).collect()
    println("Problem 01")
    cnt.foreach(println)
  }

  //Problem 02
  def numberofmedalsthatIndiawonyearwise(data:RDD[String]):Unit= {
    val counts = data.filter { x => {if(x.toString().split("\t").length >= 10) true else false} }.map(line=>{line.toString().split("\t")})
    val fil = counts.filter(x=>{if(x(2).equalsIgnoreCase("india")&&(x(9).matches(("\\d+")))) true else false })
    val pairs: RDD[(String, Int)] = fil.map(x => (x(3),x(9).toInt))
    val cnt = pairs.reduceByKey(_ + _).collect()
    println("Problem 02")
    cnt.foreach(println)
  }

  //Problem 03
  def totalnumberofmedalswonbyeachcountry(data:RDD[String]):Unit= {
    val counts = data.filter { x => {if(x.toString().split("\t").length >= 10) true else false} }.map(line=>{line.toString().split("\t")})
    val fil = counts.filter(x=>{if((x(9).matches(("\\d+")))) true else false })
    val pairs: RDD[(String, Int)] = fil.map(x => (x(2),x(9).toInt))
    val cnt = pairs.reduceByKey(_ + _).collect()
    println("Problem 03")
    cnt.foreach(println)
  }

}