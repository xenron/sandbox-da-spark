
// ---------------------------------------------------------------------------
// This example is for use with spark 1.2 CDH 5.3, it processes MNIST images
// using deep learning from h2o.
// ---------------------------------------------------------------------------

// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import hex.deeplearning.{DeepLearningModel, DeepLearning}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.h2o._
import org.apache.spark.mllib
import org.apache.spark.mllib.feature.{IDFModel, IDF, HashingTF}
import org.apache.spark.rdd.RDD

import water.Key


object h2o_spark_dl  extends App
{
  // ---------------------------------------------------------------------------
  // create a schema for a row of integers that represents an image 28x28 plus 
  // an integer element to label the row.

  def getSchema(): String = {

    var schema = ""
    val limit = 28*28

    for (i <- 1 to limit){
      schema += "P" + i.toString + " " 
    }
    schema += "Label"

    schema // return value

  }
  // ---------------------------------------------------------------------------

  // create  a spark conf and context

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "Spark h2o ex1"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  val sparkCxt = new SparkContext(conf)

  // create the h2o context 

  import org.apache.spark.h2o._
  implicit val h2oContext = new org.apache.spark.h2o.H2OContext(sparkCxt).start()

  // Initialize SQL context
  import h2oContext._
  import org.apache.spark.sql._

  implicit val sqlContext = new SQLContext(sparkCxt)

  // Open H2O UI 

  import sqlContext._
  openFlow

  // prep the IsDepDelayeefs based data

  val server    = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val path      = "/data/spark/h2o/"

  val train_csv =  server + path + "mnist_train_1x.csv"
  val test_csv  =  server + path +  "mnist_test_1x.csv"

  // load the data 

  val rawTrainData = sparkCxt.textFile(train_csv)
  val rawTestData  = sparkCxt.textFile(test_csv)

  // create a spark sql schema for the row

  val schemaString = getSchema()

  val schema = StructType( schemaString.split(" ")
      .map(fieldName => StructField(fieldName, IntegerType, false)))

  // create an RDD from the raw training data - example uses var args

  // split using var args

//  val trainRDD  = rawTrainData.map( rawRow => Row( rawRow.split(",").map(_.toInt): _* ))

  // split using fromSeq

  val trainRDD  = rawTrainData.map(rawRow => Row.fromSeq(rawRow.split(",") .map(_.toInt)))
  val testRDD   = rawTestData.map(rawRow  => Row.fromSeq(rawRow.split(",") .map(_.toInt)))

  // create a schema RDD

  val trainSchemaRDD = sqlContext.applySchema(trainRDD, schema)
  val testSchemaRDD  = sqlContext.applySchema(testRDD,  schema)

  // register schema RDD as a table 

  trainSchemaRDD.registerTempTable("trainingTable")
  testSchemaRDD.registerTempTable("testingTable")

  // now run sql against the table to filter the data

  val schemaRddTrain = sqlContext.sql("""SELECT * FROM trainingTable LIMIT 100""".stripMargin)
  val schemaRddTest  = sqlContext.sql("""SELECT * FROM testingTable LIMIT 20""".stripMargin)

  // test sql results 

  println( ">>>>> Training Data Count = " + schemaRddTrain.collect().length )

//  println( ">>>>> Training Data Count = " + schemaRddTrain.count() )
//  println( ">>>>> Testing  Data Count = " + schemaRddTest.count() )

  // Now create an h2o data frame from the filter sql result. Change the label 
  // value to an enumeration so that classification is used rather than the 
  // default regression when creating deep learning model

  val trainFrame:DataFrame = schemaRddTrain 
//  trainFrame.replace( trainFrame.find("Label"), trainFrame.vec("Label").toEnum)
//  trainFrame.update(null)

//  val testFrame:DataFrame = schemaRddTest 
//  testFrame.replace( testFrame.find("Label"), testFrame.vec("Label").toEnum)
//  testFrame.update(null)

  // Set up the deep learning parameters

//  val dlParams = new DeepLearningParameters()
//
//  dlParams._epochs               = 10
//  dlParams._train                = trainFrame
//  dlParams._valid                = testFrame
//  dlParams._response_column      = 'Label
//  dlParams._variable_importances = true

  // Train then model

//  val dl = new DeepLearning(dlParams)
//  val dlModel = dl.trainModel.get

  // determine model metrics for train and test data sets 

//  val predictTrain = dlModel.score(schemaRddTrain)('predict)
//  val predictTest  = dlModel.score(schemaRddTest)('predict)

//  val predictionsFromModel = toRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

  // print the results 

// *****************************************************************************************
// *** There is a problem with this example, it cannot be completed until a bug in h2o is 
// *** fixed and added to a release. The problem and fix details are here 
// ***     https://github.com/h2oai/sparkling-water/pull/9
// *** There was a limit of 128 elements when creating a record for deep learning. THis 
// *** example uses a record os 28*28+1 elements. Wait till fixed .... adn preferably 
// *** added to a binary release. 
// *****************************************************************************************


  // shutdown h20

//  water.H2O.shutdown()

//  sparkCxt.stop()

  println( " >>>>> Script Finished <<<<< " )

} // end application

