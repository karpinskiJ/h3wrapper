package h3xwrapper

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

package object experiments {
  case class SedonaExperimentRow(id: String, sedona_time: Double)

  case class QualityCheck(id: String, h3_resolution: Int, correctness: Double)

  case class H3ExperimentRow(id: String, h3_time: Double, h3_resolution: Int)

  def readParquet(spark: SparkSession)(path: String, datasetPath: String): DataFrame =
    spark.read.parquet(s"$path/$datasetPath")

  def writeParquet(path: String, datasetPath: String, df: DataFrame): Unit =
    df.write.mode("overwrite").parquet(s"$path/$datasetPath")

  def runSedonaExperiment(spark: SparkSession)(basePath: String, experimentId: String, experimentFunction: => DataFrame): Dataset[SedonaExperimentRow] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    writeParquet(s"$basePath/$experimentId", "SEDONA", experimentFunction)
    val endTime = System.currentTimeMillis()
    val experimentTime = (endTime - startTime) / 1000
    Seq(SedonaExperimentRow(experimentId, experimentTime)).toDS()
  }

  def runH3Experiment(spark: SparkSession)(basePath: String, experimentId: String, h3Resolution: Int, experimentFunction: Int => DataFrame): Dataset[H3ExperimentRow] = {
    import spark.implicits._
    val startTime = System.currentTimeMillis()
    writeParquet(s"$basePath/$experimentId", s"H3=$h3Resolution", experimentFunction(h3Resolution))
    val endTime = System.currentTimeMillis()
    val experimentTime = (endTime - startTime) / 1000
    Seq(H3ExperimentRow(experimentId, experimentTime, h3Resolution)).toDS()
  }


  def qualityCheck(spark: SparkSession)(basePath: String, experimentId: String, h3Resolution: Int, joinColumns: Seq[String]): Dataset[QualityCheck] = {
    import spark.implicits._
    val sedonaResults = readParquet(spark)(s"$basePath/$experimentId", "SEDONA")

    val h3Results = readParquet(spark)(s"$basePath/$experimentId", s"H3=$h3Resolution")
    val correctPercentage = sedonaResults.join(h3Results, joinColumns).count().toDouble / sedonaResults.count().toDouble * 100
    Seq(QualityCheck(experimentId, h3Resolution, correctPercentage)).toDS()

  }

  def runExperiments(spark: SparkSession)(basePath: String, experimentId: String, joinColumns: Seq[String], h3Resolutions: Seq[Int], sedonaFunction: => DataFrame, h3Function: Int => DataFrame): DataFrame = {
    val sedonaResult = runSedonaExperiment(spark)(basePath, experimentId, sedonaFunction)

    val qualityMetrics = {
      for {h3Res <- h3Resolutions
           h3Result = runH3Experiment(spark)(basePath, experimentId, h3Res, h3Function)
           quality = qualityCheck(spark)(basePath, experimentId, h3Res, joinColumns)
           } yield h3Result.join(quality, Seq("id", "h3_resolution"))
    }.reduceLeft(_.unionByName(_)).join(sedonaResult, Seq("id"))

    writeParquet(s"$basePath/$experimentId", "SUMMARY", qualityMetrics)
    qualityMetrics


  }


}
