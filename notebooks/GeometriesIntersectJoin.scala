// Databricks notebook source
// MAGIC %run /Users/jakubkarpinski615@gmail.com/Config

// COMMAND ----------

val basePath = "dbfs:/FileStore/experiments"

// COMMAND ----------

val placesDataset = places.localCheckpoint()
val bgDataset = blockGroups.localCheckpoint()

// COMMAND ----------

// DBTITLE 1,SEDONA FUN
  import org.apache.spark.sql.sedona_sql.expressions.st_predicates.{ST_Contains, ST_Intersects}

  def  geometriesIntersectJoin(geometryDataFrame1: DataFrame
                               , geometryColName1: String
                               , geometryDataFrame2: DataFrame
                               , geometryColName2: String): DataFrame = {

    geometryDataFrame1.join(geometryDataFrame2,ST_Intersects(geometryColName2,geometryColName1))
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
  def geometriesIntersectJoinH3(geometryDataFrame1: DataFrame
                              , geometryColName1: String
                              , geometryIdColName1: String
                              , geometryDataFrame2: DataFrame
                              , geometryColName2: String
                              , geometryIdColName2: String
                              , h3Resolution: Int = 7): DataFrame = {
    val geometryFilledWithH3: DataFrame = geometryDataFrame1
        .withColumn(h3_index, ST_H3CellIDs(geometryColName1, h3Resolution, true))
        .withColumn(h3_index, explode(col(h3_index)))
      

    val geometryH3Boundary: DataFrame = geometryDataFrame2
      .getGeometryBoundary(geometryColName2)
      .withColumn(h3_index, ST_H3CellIDs(s"${geometryColName2}_boundary", h3Resolution, false))
              .withColumn(h3_index,explode(col(h3_index)))
        .drop(s"${geometryColName2}_boundary")
    geometryFilledWithH3.join(geometryH3Boundary, Seq(h3_index))
      .where(ST_Intersects(geometryColName1, geometryColName2))
      .dropDuplicates(geometryIdColName1, geometryIdColName2)

  }

// COMMAND ----------

// MAGIC %md
// MAGIC # SEDONA 

// COMMAND ----------

val experimentId:String = "BLOCK_GROUP_INTERSECTS_PLACES"
def sedonaFun: DataFrame = geometriesIntersectJoin(bgDataset,"bg_shape",places, "place_shape"  )
def h3Fun(h3Resolution: Int): DataFrame = geometriesIntersectJoinH3(bgDataset,"bg_shape","bg_id",placesDataset, "place_shape","place_id",h3Resolution)

// COMMAND ----------

val sedonaResultAvg = {for(i<- 1 to 3) yield runSedonaExperiment(spark)(basePath,experimentId,sedonaFun)}.reduceLeft(_.unionByName(_)).select(avg("sedona_time").as("sedona_time") ).withColumn("id",lit(experimentId)).select("id","sedona_time") 

// COMMAND ----------

val sedonResultAvgFinal  = sedonaResultAvg.select(avg("sedona_time").as("sedona_time") ).withColumn("id",lit(experimentId)).select("id","sedona_time")  

// COMMAND ----------

// MAGIC %md
// MAGIC # H3 resolution 5

// COMMAND ----------

val res5ResultAvg = {for(i<- 1 to 3) yield runH3Experiment(spark)(basePath,experimentId,5,h3Fun) }.reduceLeft(_.unionByName(_)).select(avg("h3_time"))

// COMMAND ----------

val res5ResultAvgFinal = res5ResultAvg.select(col("avg(h3_time)").as("h3_time")).withColumn("id",lit(experimentId)).withColumn("h3_resolution",lit(5))

// COMMAND ----------

val res5qualityCheckResult = qualityCheck(spark)(basePath,experimentId,5,Seq("bg_id","place_id"))

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

val res6qualityCheckResult = qualityCheck(spark)(basePath,experimentId,6,Seq("bg_id","place_id"))

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

val res7qualityCheckResult = qualityCheck(spark)(basePath,experimentId,7,Seq("bg_id","place_id"))

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

val res8qualityCheckResult = qualityCheck(spark)(basePath,experimentId,8,Seq("bg_id","place_id"))

// COMMAND ----------

display(res8qualityCheckResult)

// COMMAND ----------

val overallResultTime = Seq(res5ResultAvgFinal,res6ResultAvgFinal,res7ResultAvgFinal,res8ResultAvgFinal).reduceLeft(_.unionByName(_)).join(sedonResultAvgFinal,Seq("id"))
val overallResultQuailty = Seq(res5qualityCheckResult,res6qualityCheckResult,res7qualityCheckResult,res8qualityCheckResult).reduceLeft(_.unionByName(_))

// COMMAND ----------

val summarry = overallResultTime.join(overallResultQuailty,Seq("id","h3_resolution"))

// COMMAND ----------

display(summarry)

// COMMAND ----------

bgDataset.join(poiDataset,ST_Contains("bg_shape","point")).explain()

// COMMAND ----------

h3Fun(6).explain()