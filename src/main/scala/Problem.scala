package org.apache.spark.ml.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.postfixOps

trait PreprocessingParam2s extends Params {
  final val isInList = new Param[Array[String]](this, "isInList", "list of isInList items")

  //TODO disable this line in order to cause the error!
  setDefault(isInList, Array[String]())
}

class ExampleTrans(override val uid: String) extends Transformer with PreprocessingParam2s {
  def this() = this(Identifiable.randomUID("testingParameter Access"))

  def copy(extra: ParamMap): ExampleTrans = {
    defaultCopy(extra)
  }

  def setIsInList(value: Array[String]): this.type = set(isInList, value)

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex("ISO")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type  ${field.dataType} did not match input type StringType")
    }
    schema.add(StructField("isInList", IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    import dataset.sparkSession.implicits._
    println("in list ", $(isInList).foreach(println))
    dataset.withColumn("isInList", when('ISO isin ($(isInList): _*), 1).otherwise(0))
  }
}

class ExampleEstimator(override val uid: String) extends Estimator[ExampleTransModel] with PreprocessingParam2s {
  def this() = this(Identifiable.randomUID("testingParameter Access"))

  def copy(extra: ParamMap): ExampleEstimator = defaultCopy(extra)

  def setIsInList(value: Array[String]): this.type = {
    println("setting value ", value.foreach(println))
    set(isInList, value)
  }

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex("ISO")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type  ${field.dataType} did not match input type StringType")
    }
    schema
      .add(StructField("isInList", IntegerType, false))
    //      .add(StructField("someField", DoubleType, false))
  }

  //in reality perform some computation here
  override def fit(dataset: Dataset[_]): ExampleTransModel = new ExampleTransModel(uid, 1.0)
}

class ExampleTransModel(
                         override val uid: String,
                         val someValue: Double
                       )
  extends Model[ExampleTransModel] with PreprocessingParam2s {

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    import dataset.sparkSession.implicits._
    println("in list ", $(isInList).foreach(println))
    dataset.withColumn("isInList", when('ISO isin ($(isInList): _*), 1).otherwise(0))
    //      .withColumn("someField", when('ISO, "fooBar"))
  }

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex("ISO")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type  ${field.dataType} did not match input type StringType")
    }
    schema
      .add(StructField("isInList", IntegerType, false))
      .add(StructField("someField", DoubleType, false))
  }

  override def copy(extra: ParamMap): ExampleTransModel = defaultCopy(extra)
}

object Foo extends App {

  Logger.getLogger("org").setLevel(Level.WARN)

  val conf: SparkConf = new SparkConf()
    .setAppName("example trans")
    .setMaster("local[*]")
    .set("spark.executor.memory", "2G")
    .set("spark.executor.cores", "4")
    .set("spark.default.parallelism", "4")
    .set("spark.driver.memory", "1G")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val dates = Seq(
    ("2016-01-01", "ABC"),
    ("2016-01-02", "ABC"),
    ("2016-01-03", "POL"),
    ("2016-01-04", "ABC"),
    ("2016-01-05", "POL"),
    ("2016-01-06", "ABC"),
    ("2016-01-07", "POL"),
    ("2016-01-08", "ABC"),
    ("2016-01-09", "def"),
    ("2016-01-10", "ABC")
  ).toDF("dates", "ISO")
  //  dates.show

  val resTransformer = new ExampleTrans().setIsInList(Array("def", "ABC")).transform(dates)
  resTransformer.show
  resTransformer.select("isInList").distinct.show
  val result = new ExampleEstimator().setIsInList(Array("def", "ABC")).fit(dates).transform(dates)
  result.show
  result.select("isInList").distinct.show

  spark.stop
}
