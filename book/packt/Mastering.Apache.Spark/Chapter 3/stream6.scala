
// Example of streaming from Apache Kafka, read rss messages and 
// store to hdfs


package nz.co.semtechsolutions


import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._


object stream6 {

  def main(args: Array[String]) {

    // check the arguments

    if ( args.length < 3 ) 
    {
      System.err.println("Usage: stream6 <brokers> <groupid> <topics>\n")
      System.err.println("<brokers> = host1:port1,host2:port2\n")
      System.err.println("<groupid> = group1\n")
      System.err.println("<topics>  = topic1,topic2\n")
      System.exit(1)
    }

    val brokers = args(0).trim
    val groupid = args(1).trim
    val topics  = args(2).trim

    println("brokers : " + brokers)
    println("groupid : " + groupid)
    println("topics  : " + topics)

    // create  a spark conf and context

    val appName = "Stream example 6"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // set up the contexts

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10) ) 

    // set up the kafka params and read from the stream

    val topicsSet = topics.split(",").toSet
    val kafkaParams : Map[String, String] = 
        Map("metadata.broker.list" -> brokers, "group.id" -> groupid )

    val rawDstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // print the stream count for debug

    rawDstream.count().map(cnt => ">>>>>>>>>>>>>>> Received events : " + cnt ).print()

   // set the hdfs storage parameters

    val now: Long = System.currentTimeMillis

    val hdfsdir = "hdfs://hc2nn:8020/data/spark/kafka/rss/"

    // format the events to a string and write them to hdfs

    val lines = rawDstream.map(record => record._2)

    lines.foreachRDD(rdd => {
	    if (rdd.count() > 0) {
              rdd.saveAsTextFile(hdfsdir+"file_"+now.toString())
            }
    })

    // start stream and await termination

    ssc.start()
    ssc.awaitTermination()

  } // end main

} // end stream6

