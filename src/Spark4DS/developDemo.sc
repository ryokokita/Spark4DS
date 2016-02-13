package Spark4DS

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object developDemo {
	// set logging options to avoid a spew of output in worksheets (don't need this in main app)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("Remoting").setLevel(Level.ERROR)
  
  // set Spark Configurations & create Spark Context (don't need this if running in spark-shell)
  val conf = new SparkConf().setMaster("local[*]").setAppName("developPrepFlightData")
                                                  //> conf  : org.apache.spark.SparkConf = org.apache.spark.SparkConf@5fcd892a
  val sc = new SparkContext(conf)                 //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@6d2d99fc
                                                  //| 
  sc.setLogLevel("ERROR")
  
  // use Spark Context to set up SQLContext
  val sqlContext = new SQLContext(sc)             //> sqlContext  : org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLCont
                                                  //| ext@31133b6e
  
  // import data using spark-csv module (good for dealing with headers & data type inferences)
  val aprilDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/rkita/Documents/Spark4DS_2016-02-13/Spark4DS/data/2015-04.csv")
                                                  //> [Stage 1:>                                                          (0 + 2
                                                  //| ) / 2]                                                                    
                                                  //|             aprilDF  : org.apache.spark.sql.DataFrame = [QUARTER: int, MON
                                                  //| TH: int, DAY_OF_WEEK: int, FL_DATE: string, AIRLINE_ID: int, CARRIER: strin
                                                  //| g, TAIL_NUM: string, FL_NUM: int, ORIGIN_AIRPORT_ID: int, ORIGIN: string, D
                                                  //| EST_AIRPORT_ID: int, DEST: string, CRS_DEP_TIME: int, DEP_TIME: int, TAXI_O
                                                  //| UT: double, WHEELS_OFF: int, WHEELS_ON: int, TAXI_IN: double, CRS_ARR_TIME:
                                                  //|  int, ARR_TIME: int, DISTANCE: double, : string]
 
  /* row transformations */
  // This UDF (user defined function) takes an integer and converts it into a float that can be used for time calculations
  val convertTime = udf((time: Integer) =>
  	if (time > 100) time.toString().dropRight(2).toString().toFloat + time.toString().takeRight(2).toFloat/60
  	else time.toString().toFloat/60
  	)                                         //> convertTime  : org.apache.spark.sql.UserDefinedFunction = UserDefinedFuncti
                                                  //| on(<function1>,FloatType,List(IntegerType))
  
  val aprilDF1 = aprilDF.withColumn("CRS_Dep_Time_f",convertTime(aprilDF("CRS_DEP_TIME")))
                                                  //> aprilDF1  : org.apache.spark.sql.DataFrame = [QUARTER: int, MONTH: int, DAY
                                                  //| _OF_WEEK: int, FL_DATE: string, AIRLINE_ID: int, CARRIER: string, TAIL_NUM:
                                                  //|  string, FL_NUM: int, ORIGIN_AIRPORT_ID: int, ORIGIN: string, DEST_AIRPORT_
                                                  //| ID: int, DEST: string, CRS_DEP_TIME: int, DEP_TIME: int, TAXI_OUT: double, 
                                                  //| WHEELS_OFF: int, WHEELS_ON: int, TAXI_IN: double, CRS_ARR_TIME: int, ARR_TI
                                                  //| ME: int, DISTANCE: double, : string, CRS_Dep_Time_f: float]
  
  aprilDF1.select("CRS_DEP_TIME","CRS_Dep_Time_f").take(5)
                                                  //> res0: Array[org.apache.spark.sql.Row] = Array([900,9.0], [900,9.0], [900,9.
                                                  //| 0], [900,9.0], [900,9.0])
  
  /* shuffle
  aprilDF1.groupBy("AIRLINE_ID").count().show()
  /* demo of groupBy on PairRDDs */
  // create PairRDD of AIRLINE_ID and count (1)
  val aprilPairRDD = aprilDF.select("AIRLINE_ID").map(id => (id,1))
  
  // do a groupBy on PairRDD (causes full shuffle)
  aprilPairRDD.groupByKey().map(t => (t._1, t._2.sum)).collect()
  
  /* demo of reduceBy on same PairRDD */
  // reduceBy does an aggregation first within each partition and then does a partial shuffle
  aprilPairRDD.reduceByKey(_ + _).collect()
  */
}