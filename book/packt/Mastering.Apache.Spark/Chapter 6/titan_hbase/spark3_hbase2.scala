
// This example is for use with spark 1.3.1, it uses direct access to 
// connect to hbase. Tries to use the clouder example at 
//
//  http://blog.cloudera.com/blog/2014/12/new-in-cloudera-labs-sparkonhbase/

package nz.co.semtechsolutions


// import spark packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.hbase._
import org.apache.hadoop.fs.Path
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Scan


object spark3_hbase2 
{

  def main(args: Array[String]) {

    // create  a spark conf and context

    val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
    val appName = "Spark HBase 2"
    val conf = new SparkConf()

    conf.setMaster(sparkMaster)
    conf.setAppName(appName)

    val sparkCxt = new SparkContext(conf)

    // create Hadoop configuration 

    val jobConf = HBaseConfiguration.create()

    val hbasePath="/opt/cloudera/parcels/CDH/etc/hbase/conf.dist/"

    jobConf.addResource(new Path(hbasePath+"hbase-site.xml"))

    val hbaseContext = new HBaseContext(sparkCxt, jobConf)

    var scan = new Scan()
    scan.setCaching(100)

    var hbaseRdd = hbaseContext.hbaseRDD("titan", scan)

    println( "Rows in Titan hbase table : " + hbaseRdd.count() )

    println( " >>>>> Script Finished <<<<< " )

  } // end main

} // end spark3_hbase2

