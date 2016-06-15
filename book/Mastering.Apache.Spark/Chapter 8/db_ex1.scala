
// ---------------------------------------------------------------------------
// This is a simple scala example to enable data brick library import 
// functionality to be demonstrated. The scala URL comes from a generated 
// value provided by a data bricks cluster. 
// ---------------------------------------------------------------------------

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object db_ex1  extends App
{

  // create  a spark conf and context

//  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"

  val appName = "Data Bricks example 1"
  val conf = new SparkConf()

//  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  val sparkCxt = new SparkContext(conf)

  var seed1:BigInt = 1
  var seed2:BigInt = 1
  val limit = 100

  var resultStr = seed1 + " " + seed2 + " "

  for( i <- 1 to limit ){

    val fib:BigInt = seed1 + seed2 

    resultStr += fib.toString + " " 

    seed1 = seed2
    seed2 = fib
  }

  println()
  println( "Result : " + resultStr )
  println()

  sparkCxt.stop()

} // end application

