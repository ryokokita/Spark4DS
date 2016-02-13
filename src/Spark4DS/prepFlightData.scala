package Spark4DS

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object prepFlightData extends App {
  // set Spark Configurations & create Spark Context (don't need this if running in spark-shell)
  val conf = new SparkConf().setAppName("prepFlightData")
  val sc = new SparkContext(conf)
  
  // use Spark Context to set up SQLContext
  val sqlContext = new SQLContext(sc)
  
  // import data using spark-csv module (good for dealing with headers & data type inferences)
  val aprilDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/rkita/Documents/Spark4DS_2016-02-13/Spark4DS/data/2015-04.csv")
 
  val mayDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // this data set doesn't have headers
      .option("inferSchema", "true")
      .load("/Users/rkita/Documents/Spark4DS_2016-02-13/Spark4DS/data/2015-05.csv")

  val juneDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/rkita/Documents/Spark4DS_2016-02-13/Spark4DS/data/2015-06.csv")

  // concatanate the three data sets using DataFrame function
  val comboDF = aprilDF.unionAll(mayDF).unionAll(juneDF)
  

  /* row transformations */
  // Code time UDFs (user defined functions) to apply to data set
  // Any Scala functional programming methods can be used in these UDFs (i.e. case matching, etc.)
  
  // The first UDF takes an integer and converts it into a float that can be used for time calculations
  val convertTime = udf((time: Integer) =>
    if (time > 100) time.toString().dropRight(2).toString().toFloat + time.toString().takeRight(2).toFloat/60
    else time.toString().toFloat/60
    )
  
  // The second UDF calculates the difference between arrival and departure times
  // Note that this UDF can be used to calculate both the estimated and actual times since we can pass both sets of data to the same UDF
  val totalTime = udf((depT: Float, arrT: Float) => arrT - depT)
  
  // The third and fourth UDFs define various metrics for "lateness"
  val late = udf((crsT: Float, arrT: Float) => if (arrT > crsT) 1 else 0)
  val longerTT = udf((crsTT: Float, actualTT: Float) => if (actualTT > crsTT) 1 else 0)


  // Larger data set: convert time columns to floats for calculations
  val comboDF1 = comboDF.withColumn("CRS_Dep_Time_f",convertTime(comboDF("CRS_DEP_TIME")))
    .withColumn("Dep_Time_f",convertTime(comboDF("DEP_TIME")))
    .withColumn("CRS_Arr_Time_f",convertTime(comboDF("CRS_ARR_TIME")))
    .withColumn("Arr_Time_f",convertTime(comboDF("ARR_TIME")))
  
  // calculate total time values
  val comboDF2 = comboDF1.withColumn("CRS_Total_Time",totalTime(comboDF1("CRS_Dep_Time_f"),comboDF1("CRS_Arr_Time_f")))
    .withColumn("Actual_Total_Time",totalTime(comboDF1("Dep_Time_f"),comboDF1("Arr_Time_f")))
  
  // apply "late" logic
  val comboDF3 = comboDF2.withColumn("late",late(comboDF2("CRS_Arr_Time_f"),comboDF2("Arr_Time_f")))
    .withColumn("longerTT",longerTT(comboDF2("CRS_Total_Time"),comboDF2("Actual_Total_Time")))


  // join larger tables
  // create temp table for access using SQL
  comboDF3.registerTempTable("comboSQL")
  
  // SQL join
  val flightStatsDF = sqlContext.sql("""
    SELECT
      t1.AIRLINE_ID,
      t1.FL_NUM,
      t1.TAIL_NUM,
      t1.DEST_AIRPORT_ID,
      t1.CRS_Arr_Time_f,
      t2.ORIGIN_AIRPORT_ID,
      t2.CRS_Dep_Time_f
    FROM comboSQL as t1
    INNER JOIN comboSQL as t2
    ON t1.DAY_OF_WEEK = t2.DAY_OF_WEEK AND t1.AIRLINE_ID = t2.AIRLINE_ID AND t1.TAIL_NUM = t2.TAIL_NUM AND t1.FL_NUM = t2.FL_NUM
    WHERE t1.CRS_Arr_Time_f < t2.CRS_Dep_Time_f
  """)
  
  /* save flightStatsDF to disk for future analysis */ 
  // coalesce brings the table into one output partition, which is handy when working locally (can delete if working in distrbuted mode)
  flightStatsDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/rkita/Documents/Spark4DS_2016-02-13/Spark4DS/data/flightStats.csv")
}