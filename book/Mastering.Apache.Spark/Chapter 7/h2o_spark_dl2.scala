
// ---------------------------------------------------------------------------
// This example is for use with spark 1.2 CDH 5.3, it processes Canadian
// income data using deep learning from h2o.
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


object h2o_spark_dl2  extends App
{

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

  val train_csv =  server + path + "adult.train.data" // 32,562 rows
  val test_csv  =  server + path + "adult.test.data"  // 16,283 rows

  // load the data 

  val rawTrainData = sparkCxt.textFile(train_csv)
  val rawTestData  = sparkCxt.textFile(test_csv)

  // create a spark sql schema for the row

  val schemaString = "age workclass fnlwgt education educationalnum maritalstatus" +
                     " occupation relationship race gender capitalgain capitalloss" +
                     " hoursperweek nativecountry income"

  val schema = StructType( schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true)))

  // create an RDD from the raw training data 

  val trainRDD  = rawTrainData
         .filter(!_.isEmpty)
         .map(_.split(",")) 
         .filter( rawRow => ! rawRow(14).trim.isEmpty )
         .map(rawRow => Row(
                             rawRow(0).toString.trim,  rawRow(1).toString.trim,
                             rawRow(2).toString.trim,  rawRow(3).toString.trim,
                             rawRow(4).toString.trim,  rawRow(5).toString.trim,
                             rawRow(6).toString.trim,  rawRow(7).toString.trim,
                             rawRow(8).toString.trim,  rawRow(9).toString.trim,
                             rawRow(10).toString.trim, rawRow(11).toString.trim,
                             rawRow(12).toString.trim, rawRow(13).toString.trim,
                             rawRow(14).toString.trim
                           )
             )

  println( ">>>>> Raw Training Data Count = " + trainRDD.count() )

  val testRDD  = rawTestData
         .filter(!_.isEmpty)
         .map(_.split(",")) 
         .filter( rawRow => ! rawRow(14).trim.isEmpty )
         .map(rawRow => Row(
                             rawRow(0).toString.trim,  rawRow(1).toString.trim,
                             rawRow(2).toString.trim,  rawRow(3).toString.trim,
                             rawRow(4).toString.trim,  rawRow(5).toString.trim,
                             rawRow(6).toString.trim,  rawRow(7).toString.trim,
                             rawRow(8).toString.trim,  rawRow(9).toString.trim,
                             rawRow(10).toString.trim, rawRow(11).toString.trim,
                             rawRow(12).toString.trim, rawRow(13).toString.trim,
                             rawRow(14).toString.trim
                           )
             )

  println( ">>>>> Raw Testing Data Count = " + testRDD.count() )

  // create a schema RDD

  val trainSchemaRDD = sqlContext.applySchema(trainRDD, schema)
  val testSchemaRDD  = sqlContext.applySchema(testRDD,  schema)

  // register schema RDD as a table 

  trainSchemaRDD.registerTempTable("trainingTable")
  testSchemaRDD.registerTempTable("testingTable")

  println( ">>>>> Schema RDD Training Data Count = " + trainSchemaRDD.count() )
  println( ">>>>> Schema RDD Testing Data Count  = " + testSchemaRDD.count() )

  // now run sql against the table to filter the data

  val schemaRddTrain = sqlContext.sql(
    """SELECT 
         |age,workclass,education,maritalstatus,
         |occupation,relationship,race,
         |gender,hoursperweek,nativecountry,income 
         |FROM trainingTable """.stripMargin)

  val schemaRddTest = sqlContext.sql(
    """SELECT 
         |age,workclass,education,maritalstatus,
         |occupation,relationship,race,
         |gender,hoursperweek,nativecountry,income 
         |FROM testingTable """.stripMargin)

  // test sql results 

  println( ">>>>> Training Data Count = " + schemaRddTrain.count() )
  println( ">>>>> Testing  Data Count = " + schemaRddTest.count() )

  // Now create an h2o data frame from the filter sql result. Change the label 
  // value to an enumeration so that classification is used rather than the 
  // default regression when creating deep learning model

  val trainFrame:DataFrame = schemaRddTrain 
  trainFrame.replace( trainFrame.find("income"),        trainFrame.vec("income").toEnum)
  trainFrame.update(null)

  val testFrame:DataFrame = schemaRddTest 
  testFrame.replace( testFrame.find("income"),        testFrame.vec("income").toEnum)
  testFrame.update(null)

  // set up the enumerated test data column so that it can be used 
  // later to create an accuracy score

  val testResArray = schemaRddTest.collect()
  val sizeResults  = testResArray.length
  var resArray     = new Array[Double](sizeResults)

  for ( i <- 0 to ( resArray.length - 1)) {
     resArray(i) = testFrame.vec("income").at(i)
  }

  // Set up the deep learning parameters

  val dlParams = new DeepLearningParameters()

  dlParams._epochs               = 100
  dlParams._train                = trainFrame
  dlParams._valid                = testFrame
  dlParams._response_column      = 'income
  dlParams._variable_importances = true

  // Train then model

  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get

  // determine model metrics for train and test data sets 

  val testH2oPredict  = dlModel.score(schemaRddTest )('predict)

  val testPredictions  = toRDD[DoubleHolder](testH2oPredict)
          .collect.map(_.result.getOrElse(Double.NaN))

  // calculate the accuracy of the test results and print

  var resAccuracy = 0
  for ( i <- 0 to ( resArray.length - 1)) {
    if (  resArray(i) == testPredictions(i) ) 
      resAccuracy = resAccuracy + 1
  }

  println()
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" )
  println( ">>>>>> Model Test Accuracy = " + 100*resAccuracy / resArray.length  + " % " )
  println( ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" )
  println()

  // shutdown h20

  water.H2O.shutdown()

  sparkCxt.stop()

  println( " >>>>> Script Finished <<<<< " )

} // end application

