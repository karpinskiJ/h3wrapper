package h3xwrapper.predicate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.{ST_Contains, ST_Intersects}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_SimplifyPreserveTopology, ST_Distance}
import h3xwrapper.utils.Spatial

package object sedona {

  def pointInPolygonJoin(polygonsDataFrame: DataFrame
                         , polygonColName: String
                         , pointsDataFrame: DataFrame
                         , pointColName: String
                        ): DataFrame =
    polygonsDataFrame
      .join(pointsDataFrame, ST_Contains(polygonColName, pointColName))


  def geometryInsidePolygonJoin(geometryDataFrame: DataFrame
                                , geometryColName: String
                                , polygonDataFrame: DataFrame
                                , polygonColName: String): DataFrame = {
    geometryDataFrame.join(polygonDataFrame, ST_Contains(polygonColName, geometryColName))
  }

  def geometriesIntersectJoin(geometryDataFrame1: DataFrame
                              , geometryColName1: String
                              , geometryDataFrame2: DataFrame
                              , geometryColName2: String): DataFrame = {
    geometryDataFrame1.join(geometryDataFrame2, ST_Intersects(geometryColName2, geometryColName1))
  }

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

  def sedonaGetGeometryInRangeFromPolygon(polygonsDfSource: DataFrame
                                          , sourceGeometryCol: String
                                          , geometryDfTarget: DataFrame
                                          , targetGeometryCol: String
                                          , range: Double
                                          , sourceCrs: String = "epsg:4326"
                                          , targetCrs: String = "epsg:2163"
                                          , distanceTolerance: Double = 0.001
                                         ): DataFrame = {

    val sourceGeometryInMeterColName = s"meter_$sourceGeometryCol"
    val targetGeometryInMeterColName = s"meter_$targetGeometryCol"
    polygonsDfSource.getGeometrySimplified(sourceGeometryCol, distanceTolerance)
      .transformCrs(sourceGeometryInMeterColName, sourceGeometryCol, targetCrs, sourceCrs)
      .join(geometryDfTarget
        .transformCrs(targetGeometryInMeterColName, targetGeometryCol, targetCrs, sourceCrs)
      )
      .where(ST_Distance(sourceGeometryInMeterColName, targetGeometryInMeterColName) <= range)
      .drop(sourceGeometryInMeterColName, targetGeometryInMeterColName)
  }


}
