
// Parquet based example code

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType,StructField,StringType};


object sql2 {

  def main(args: Array[String]) {

    // create  a spark conf and context

    val appName = "sql example 2"
    val conf    = new SparkConf()

    conf.setAppName(appName)

    // create contexts

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // load some data from hdfs 

    val rawRdd = sc.textFile("hdfs:///data/spark/sql/adult.test.data_1x")

    // specify a schema

    val schemaString = "age workclass fnlwgt education educational-num " +
                       "marital-status occupation relationship race gender " +
                       "capital-gain capital-loss hours-per-week native-country income"

    val schema =
      StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // create row data

    val rowRDD = rawRdd.map(_.split(","))
      .map(p => Row( p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8),
                      p(9),p(10),p(11),p(12),p(13),p(14) ))

    // create a data frame from schema and row data

    val adultDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // now save the data in parquet format

    adultDataFrame.save("hdfs:///data/spark/sql/adult.parquet","parquet")

  } // end main

} // end sql2

