
// This is an examle application used with Spark 1.3.1 to test 
// Spark access to raw cassandra table data.

package nz.co.semtechsolutions


// import spark packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// datastax cassandra classes ( using datastax connector )

import com.datastax.spark.connector._


object spark3_cass 
{

  def main(args: Array[String]) {

    // create  a spark conf and context

    val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
    val appName = "Spark Cass 1"
    val conf = new SparkConf()

    // set up spark configuration

    conf.setMaster(sparkMaster)
    conf.setAppName(appName)

    // set up cassandra configuration

    conf.set("spark.cassandra.connection.host", "hc2r1m2")

    val sparkCxt = new SparkContext(conf)

    // access cassandra directly from spark 

    val keySpace =  "titan"
    val tableName = "edgestore"
 
    val cassRDD = sparkCxt.cassandraTable( keySpace, tableName )

    println( "Cassandra Table Rows : " + cassRDD.count )

    println( " >>>>> Script Finished <<<<< " )

  } // end main

} // end spark3_cass

