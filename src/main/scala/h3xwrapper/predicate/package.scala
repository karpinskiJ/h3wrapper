package h3xwrapper

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, explode, size}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_H3KRing
import h3xwrapper.Constants.h3_index
import h3xwrapper.utils.{Spatial, getH3EdgeLength}

import scala.math

package object predicate {


  def polygonContainsPointJoin(polygonDf: DataFrame,
                               polygonIdColName: String,
                               polygonColName: String,
                               pointDf: DataFrame,
                               pointIdColName: String,
                               pointColName: String, h3Resolution: Option[Int] = None): DataFrame = {
    val (polygonDfTransformed, pointDfTransformed): (DataFrame, DataFrame) = h3Resolution match {
      case Some(resolution) => (polygonDf.getGeometryInH3Exploded(polygonColName, resolution), pointDf.getPointInH3(pointColName, resolution))
      case None => (polygonDf.withColumn(h3_index, explode(col(h3_index))), pointDf)
    }
    polygonDfTransformed.join(pointDfTransformed,
      Seq(h3_index)
    ).dropDuplicates(polygonIdColName, pointIdColName).drop(h3_index)
  }


  def polygonContainsPolygonJoin(outerPolygonDf: DataFrame,
                                 outerPolygonIdColName: String,
                                 outerPolygonColName: String,
                                 innerPolygonDf: DataFrame,
                                 innerPolygonIdColName: String,
                                 innerPolygonColName: String,
                                 h3Resolution: Option[Int] = None
                                ): DataFrame = {

    def transformInnerPolygonDf(df: DataFrame): DataFrame =
      df.withColumn("h3_indexes_number", size(col(h3_index)))
        .withColumn(h3_index, explode(col(h3_index)))


    val (outerPolygonDfTransformed, innerPolygonDfTransformed): (DataFrame, DataFrame) = h3Resolution match {
      case Some(resolution) => (outerPolygonDf.getGeometryInH3Exploded(outerPolygonColName, resolution),
        transformInnerPolygonDf(innerPolygonDf.getGeometryBoundaryInH3(innerPolygonColName, resolution)))
      case None => (outerPolygonDf.withColumn(h3_index, explode(col(h3_index))), transformInnerPolygonDf(innerPolygonDf))
    }
    outerPolygonDfTransformed
      .join(innerPolygonDfTransformed, Seq(h3_index))
      .groupBy(outerPolygonIdColName, innerPolygonIdColName, "h3_indexes_number")
      .agg(count(innerPolygonIdColName).as("h3_indexes_matched"))
      .where(col("h3_indexes_matched") === col("h3_indexes_number"))
      .drop("h3_indexes_number", "h3_indexes_matched")

  }


  def polygonIntersectsPolygon(filledPolygonDf: DataFrame
                               , filledPolygonIdColName: String
                               , filledPolygonColName: String
                               , boundaryPolygonDf: DataFrame
                               , boundaryPolygonId: String
                               , boundaryColName: String
                               , h3Resolution: Option[Int] = None): DataFrame = {

    val (filledPolygonDfTransformed, boundaryPolygonDfTransformed): (DataFrame, DataFrame) = h3Resolution match {
      case Some(resolution) => (filledPolygonDf.getGeometryInH3Exploded(filledPolygonColName, resolution),
        boundaryPolygonDf.getGeometryBoundaryInH3(boundaryColName, resolution).withColumn(h3_index, explode(col(h3_index)))
      )
      case None => (filledPolygonDf.withColumn(h3_index, explode(col(h3_index))), boundaryPolygonDf.withColumn(h3_index, explode(col(h3_index))))
    }
    filledPolygonDfTransformed.join(boundaryPolygonDfTransformed, Seq(h3_index)).dropDuplicates(filledPolygonIdColName, boundaryPolygonId)
  }

  def getPointsInRangeFromPoints(pointsDfSource: DataFrame
                                 , sourceIdCol: String
                                 , sourceGeometryCol: String
                                 , pointsDfTarget: DataFrame
                                 , targetIdCol: String
                                 , targetGeometryCol: String
                                 , range: Double
                                 , h3Resolution: Int): DataFrame = {

    val k: Int = transformMetersToK(range, h3Resolution)
    val pointsDfSourceTransformedToH3Range: DataFrame = pointsDfSource
      .getPolygonInH3(sourceGeometryCol, h3Resolution)
      .withColumn(h3_index, col(h3_index).getItem(0))
      .withColumn(h3_index, ST_H3KRing(col(h3_index), k, false))
      .withColumn(h3_index, explode(col(h3_index)))
    val pointsDfTargetTransformedToH3: DataFrame = pointsDfTarget.getPolygonInH3(targetGeometryCol, h3Resolution).withColumn(h3_index, col(h3_index).getItem(0))
    pointsDfSourceTransformedToH3Range.join(pointsDfTargetTransformedToH3, Seq(h3_index))
      .dropDuplicates(sourceIdCol, targetIdCol)

  }

  def transformMetersToK(range: Double, h3Resolution: Int): Int =
    (math.sqrt(3) * (range - 1) / (getH3EdgeLength(h3Resolution) * 3)).toInt

}
