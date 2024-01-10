package h3xwrapper.predicate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.{ST_Contains, ST_Intersects}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_SimplifyPreserveTopology, ST_Distance}
import h3xwrapper.utils.Spatial

package object sedona {
  /**
   * Performs a spatial join operation between polygons and points based on spatial containment.
   *
   * @param polygonsDataFrame  The DataFrame containing polygons.
   * @param polygonColName     The name of the column containing polygon geometries.
   * @param pointsDataFrame    The DataFrame containing points.
   * @param pointColName       The name of the column containing point geometries.
   * @return                   A DataFrame with joined data based on spatial containment.
   */
  def pointInPolygonJoin(polygonsDataFrame: DataFrame
                         , polygonColName: String
                         , pointsDataFrame: DataFrame
                         , pointColName: String
                        ): DataFrame =
    polygonsDataFrame
      .join(pointsDataFrame, ST_Contains(polygonColName, pointColName))

  /**
   * Performs a spatial join operation between geometries and polygons based on spatial containment.
   *
   * @param geometryDataFrame   The DataFrame containing geometries.
   * @param geometryColName     The name of the column containing geometry geometries.
   * @param polygonDataFrame    The DataFrame containing polygons.
   * @param polygonColName      The name of the column containing polygon geometries.
   * @return                    A DataFrame with joined data based on spatial containment.
   */
  def geometryInsidePolygonJoin(geometryDataFrame: DataFrame
                                , geometryColName: String
                                , polygonDataFrame: DataFrame
                                , polygonColName: String): DataFrame = {
    geometryDataFrame.join(polygonDataFrame, ST_Contains(polygonColName, geometryColName))
  }

  /**
   * Performs a spatial join operation between two sets of geometries based on spatial intersection.
   *
   * @param geometryDataFrame1 The first DataFrame containing geometries.
   * @param geometryColName1   The name of the column containing geometries in the first DataFrame.
   * @param geometryDataFrame2 The second DataFrame containing geometries.
   * @param geometryColName2   The name of the column containing geometries in the second DataFrame.
   * @return A DataFrame with joined data based on spatial intersection.
   */
  def geometriesIntersectJoin(geometryDataFrame1: DataFrame
                              , geometryColName1: String
                              , geometryDataFrame2: DataFrame
                              , geometryColName2: String): DataFrame = {
    geometryDataFrame1.join(geometryDataFrame2, ST_Intersects(geometryColName2, geometryColName1))
  }

  /**
   * Retrieves points from a source DataFrame that fall within a specified range from points in a target DataFrame.
   *
   * @param pointsDfSource        The source DataFrame containing points.
   * @param sourceGeometryCol     The name of the column containing point geometries in the source DataFrame.
   * @param pointsDfTarget        The target DataFrame containing points.
   * @param targetGeometryCol     The name of the column containing point geometries in the target DataFrame.
   * @param range                 The spatial range in meters.
   * @param sourceCrs             The coordinate reference system of the source DataFrame (default is "epsg:4326").
   * @param targetCrs             The coordinate reference system of the target DataFrame (default is "epsg:2163").
   * @return                      A DataFrame with points that meet the spatial criteria.
   */
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

  /**
   * Retrieves geometries from a source DataFrame that fall within a specified range from polygons in a target DataFrame.
   *
   * @param polygonsDfSource      The source DataFrame containing polygons.
   * @param sourceGeometryCol     The name of the column containing polygon geometries in the source DataFrame.
   * @param geometryDfTarget      The target DataFrame containing geometries.
   * @param targetGeometryCol     The name of the column containing geometry geometries in the target DataFrame.
   * @param range                 The spatial range in meters.
   * @param sourceCrs             The coordinate reference system of the source DataFrame (default is "epsg:4326").
   * @param targetCrs             The coordinate reference system of the target DataFrame (default is "epsg:2163").
   * @param distanceTolerance     The tolerance for simplifying source geometries (default is 0.001).
   * @return                      A DataFrame with geometries that meet the spatial criteria.
   */
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
