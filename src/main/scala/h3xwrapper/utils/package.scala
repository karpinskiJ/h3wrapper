package h3xwrapper

import com.uber.h3core.LengthUnit
import h3xwrapper.Constants.h3_index
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.locationtech.jts.geom.Geometry
import scala.jdk.CollectionConverters.asScalaBufferConverter

package object utils {

  def loadShapefileToDf(path: String)(implicit spark: SparkSession): DataFrame = {
    val rdd: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
    Adapter.toDf(rdd, spark)
  }

  implicit class Spatial(df: DataFrame) {

    def getH3Range(pointColName: String, h3Resolution: Int, k: Int): DataFrame = {
      df
        .getPointInH3(pointColName, h3Resolution)
        .withColumn(h3_index, explode(ST_H3KRing(col(h3_index), k, false)))
    }


    def addGeometryCentroidColumn(geometryColName: String, centroidColName: String): DataFrame = {
      df.withColumn(centroidColName, ST_Centroid(geometryColName))
    }

    def transformCrs(targetColName: String, sourceColName: String, targetCrs: String = "epsg:2163", sourceCrs: String = "epsg:4326"): DataFrame =
      df.withColumn(targetColName, expr(s"ST_Transform($sourceColName,'$sourceCrs','$targetCrs')"))


    def getGeometryBoundary(geometryColName: String): DataFrame =
      df.withColumn(s"${geometryColName}_boundary", ST_Boundary(geometryColName))

    def getGeometrySimplified(geometryColName: String, distanceTolerance: Double): DataFrame =
      df.withColumn(geometryColName, ST_SimplifyPreserveTopology(geometryColName, distanceTolerance))

    def getPolygonInH3(polygonColName: String, h3Resolution: Int): DataFrame = {
      df.withColumn(h3_index, ST_H3CellIDs(polygonColName, h3Resolution, true))
    }

    def getPointInH3(pointColName: String, h3Resolution: Int): DataFrame = {
      df.withColumn(h3_index, ST_H3CellIDs(pointColName, h3Resolution, false).getItem(0))
    }

    def getGeometryInH3Exploded(geometryColName: String, h3Resolution: Int): DataFrame = {
      df.getPolygonInH3(geometryColName, h3Resolution)
        .withColumn(h3_index, explode(col(h3_index)))
    }


    def createH3ShapeColumn(): DataFrame =
      df.withColumn(s"${h3_index}_shape", getH3Shape(col(h3_index)))


  }

  def getH3EdgeLength(h3Resolution: Int): Double = H3.instance.getHexagonEdgeLengthAvg(h3Resolution, LengthUnit.m)

  private def getH3Shape(h3Index: Column): Column = {
    val getH3ShapeUdf = udf { (h3Index: Long) => {
      val shape = H3.instance.cellToBoundary(h3Index)
      val coordinates: List[String] = shape.asScala.toList.map(point => s"${point.lng} ${point.lat}")
      val polygonString: String = {
        coordinates ++ coordinates.take(1)
      }.toString.drop(4)
      s"POLYGON(${polygonString})".stripMargin
    }
    }
    getH3ShapeUdf(h3Index)
  }

}



