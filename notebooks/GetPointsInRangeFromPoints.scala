// Databricks notebook source
// MAGIC %run /Users/jakubkarpinski615@gmail.com/Config

// COMMAND ----------

val basePath = "dbfs:/FileStore/experiments"

// COMMAND ----------

val poiDataset = poi.localCheckpoint()
val placeDataset =placesCentroids.localCheckpoint()

// COMMAND ----------

// DBTITLE 1,SEDONA FUN

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.{ST_Contains, ST_Intersects}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_SimplifyPreserveTopology,ST_Distance}
import h3xwrapper.utils.Spatial
def sedonaGetPointsInRangeFromPoints(pointsDfSource: DataFrame
                                       , sourceGeometryCol: String
                                       , pointsDfTarget: DataFrame
                                       , targetGeometryCol: String
                                       , range: Double
                                       , sourceCrs: String = "epsg:4326"
                                       , targetCrs: String = "epsg:2163"
                                      ): DataFrame = {
    val sourceGeometryInMeterColName = s"meter_$sourceGeometryCol"
    val targetGeometryInMeterColName = s"meter_$targetGeometryCol"

    pointsDfSource.transformCrs(sourceGeometryInMeterColName, sourceGeometryCol, targetCrs, sourceCrs)
      .join(pointsDfTarget.transformCrs(targetGeometryInMeterColName, targetGeometryCol, targetCrs, sourceCrs)
      ).where(ST_Distance(sourceGeometryInMeterColName, targetGeometryInMeterColName) <= range)
      .drop(sourceGeometryInMeterColName, targetGeometryInMeterColName)

  }

// COMMAND ----------

// DBTITLE 1,H3 FUN
import com.uber.h3core.LengthUnit
import h3xwrapper.Constants.h3_index
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.Geometry
import h3xwrapper.utils.Spatial
import com.uber.h3core.LengthUnit
import h3xwrapper.Constants.h3_index
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import h3xwrapper.Constants.{geometry_centroid, h3_index}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.Geometry

import scala.jdk.CollectionConverters.asScalaBufferConverter
import h3xwrapper.utils.getH3EdgeLength
  def getPointsInRangeFromPoints(pointsDfSource: DataFrame
                                 , sourceGeometryCol: String
                                 , sourceIdCol: String
                                 , pointsDfTarget: DataFrame
                                 , targetGeometryCol: String
                                 , targetIdCol: String
                                 , range: Double
                                 , h3Resolution: Int
                                 , sourceCrs: String = "epsg:4326"
                                 , targetCrs: String = "epsg:2163"
                                ): DataFrame = {
    val sourceGeometryInMeterColName = s"meter_$sourceGeometryCol"
    val targetGeometryInMeterColName = s"meter_$targetGeometryCol"
    def transformMetersToK(range: Double, h3Resolution: Int): Int =
      (math.sqrt(3) * (range - 1) / (getH3EdgeLength(h3Resolution) * 3)).toInt

    val k: Int = transformMetersToK(range, h3Resolution) + 1
    val pointsDfSourceTransformedToH3Range: DataFrame = pointsDfSource
      .getH3Range(sourceGeometryCol, h3Resolution, k)

    val pointsDfTargetTransformedToH3: DataFrame = pointsDfTarget
      .getPointInH3(targetGeometryCol, h3Resolution)

    pointsDfSourceTransformedToH3Range.join(pointsDfTargetTransformedToH3, Seq(h3_index))
      .transformCrs(sourceGeometryInMeterColName, sourceGeometryCol, targetCrs, sourceCrs)
      .transformCrs(targetGeometryInMeterColName, targetGeometryCol, targetCrs, sourceCrs)
      .where(ST_Distance(sourceGeometryCol, targetGeometryCol) <= range)
      .drop(sourceGeometryInMeterColName,targetGeometryInMeterColName )


  }

// COMMAND ----------

// MAGIC %md
// MAGIC # SEDONA 

// COMMAND ----------

val experimentId:String = "POINT_IN_RANGE_FROM_POINT"
def sedonaFun: DataFrame = sedonaGetPointsInRangeFromPoints(poiDataset,"point",placeDataset,"place_point",10000)
def h3Fun(h3Resolution: Int): DataFrame = getPointsInRangeFromPoints(
  poiDataset,"point","point_id",placeDataset,"place_point","place_id",10000,h3Resolution
)
 

// COMMAND ----------

import h3xwrapper.experiments.{runSedonaExperiment, runH3Experiment,qualityCheck}

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

// COMMAND ----------

val sedonaResultAvg = {for(i<- 1 to 3) yield runSedonaExperiment(spark)(basePath,experimentId,sedonaFun) }.reduceLeft(_.unionByName(_)).select(avg("sedona_time").as("sedona_time") ).withColumn("id",lit(experimentId)).select("id","sedona_time") 

// COMMAND ----------

val sedonResultAvgFinal  = sedonaResultAvg.select(avg("sedona_time").as("sedona_time") ).withColumn("id",lit(experimentId)).select("id","sedona_time")  

// COMMAND ----------

display(sedonResultAvgFinal)

// COMMAND ----------

// MAGIC %md
// MAGIC # H3 resolution 5

// COMMAND ----------

val res5ResultAvg = {for(i<- 1 to 3) yield runH3Experiment(spark)(basePath,experimentId,5,h3Fun) }.reduceLeft(_.unionByName(_)).select(avg("h3_time"))

// COMMAND ----------

val res5ResultAvgFinal = res5ResultAvg.select(col("avg(h3_time)").as("h3_time")).withColumn("id",lit(experimentId)).withColumn("h3_resolution",lit(5))

// COMMAND ----------

val res5qualityCheckResult = qualityCheck(spark)(basePath,experimentId,5,Seq("point_id","place_id"))

// COMMAND ----------

display(res5ResultAvgFinal)

// COMMAND ----------

display(res5qualityCheckResult)

// COMMAND ----------

// MAGIC %md
// MAGIC # H3 Resolution 6

// COMMAND ----------

val res6ResultAvg = {for(i<- 1 to 3) yield runH3Experiment(spark)(basePath,experimentId,6,h3Fun) }.reduceLeft(_.unionByName(_)).select(avg("h3_time").as("h3_time"))

// COMMAND ----------

val res6ResultAvgFinal = res6ResultAvg.withColumn("id",lit(experimentId)).withColumn("h3_resolution",lit(6))

// COMMAND ----------

display(res6ResultAvgFinal)

// COMMAND ----------

val res6qualityCheckResult = qualityCheck(spark)(basePath,experimentId,6,Seq("point_id","place_id"))

// COMMAND ----------

display(res6qualityCheckResult)

// COMMAND ----------

// MAGIC %md
// MAGIC # H3 Resolution 7

// COMMAND ----------

val res7ResultAvg = {for(i<- 1 to 3) yield runH3Experiment(spark)(basePath,experimentId,7,h3Fun) }.reduceLeft(_.unionByName(_)).select(avg("h3_time").as("h3_time"))

// COMMAND ----------

val res7ResultAvgFinal = res7ResultAvg.withColumn("id",lit(experimentId)).withColumn("h3_resolution",lit(7))

// COMMAND ----------

display(res7ResultAvgFinal)

// COMMAND ----------

val res7qualityCheckResult = qualityCheck(spark)(basePath,experimentId,7,Seq("point_id","place_id"))

// COMMAND ----------

display(res7qualityCheckResult)

// COMMAND ----------

// MAGIC %md
// MAGIC # H3 Resolution 8

// COMMAND ----------

val res8ResultAvg = {for(i<- 1 to 3) yield runH3Experiment(spark)(basePath,experimentId,8,h3Fun) }.reduceLeft(_.unionByName(_)).select(avg("h3_time").as("h3_time"))

// COMMAND ----------

val res8ResultAvgFinal = res8ResultAvg.withColumn("id",lit(experimentId)).withColumn("h3_resolution",lit(8))

// COMMAND ----------

display(res8ResultAvgFinal)

// COMMAND ----------

val res8qualityCheckResult = qualityCheck(spark)(basePath,experimentId,8,Seq("place_id","point_id"))

// COMMAND ----------

display(res8qualityCheckResult)

// COMMAND ----------

val overallResultTime = Seq(res5ResultAvgFinal,res6ResultAvgFinal,res7ResultAvgFinal,res8ResultAvgFinal).reduceLeft(_.unionByName(_)).join(sedonResultAvgFinal,Seq("id"))
val overallResultQuailty = Seq(res5qualityCheckResult,res6qualityCheckResult,res7qualityCheckResult,res8qualityCheckResult).reduceLeft(_.unionByName(_))

// COMMAND ----------

val summarry = overallResultTime.join(overallResultQuailty,Seq("id","h3_resolution"))

// COMMAND ----------

display(summarry)