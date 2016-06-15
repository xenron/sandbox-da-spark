
// ---------------------------------------------------------------------------
// This example is for use with spark 1.2 CDH 5.3 it is just intended to start
// the Flow interface so that it can be access. H2O and Spark stop have been
// commented out on purpose.
// ---------------------------------------------------------------------------

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.h2o._


object h2o_spark_ex2  extends App
{
  // create  a spark conf and context

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "Spark h2o ex2"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  val sparkCxt = new SparkContext(conf)

  // create the h2o context 

  implicit val h2oContext = new org.apache.spark.h2o.H2OContext(sparkCxt).start()

  import h2oContext._

  // Open H2O UI 

  openFlow

  // left the app / H2O open on purpose so that I could access H2O Flow.

  // shutdown h20

//  water.H2O.shutdown()

//  sparkCxt.stop()

  println( " >>>>> Script Finished <<<<< " )

} // end application

