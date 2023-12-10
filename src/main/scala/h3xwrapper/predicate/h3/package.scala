package h3xwrapper.predicate

import h3xwrapper.Constants.{geometry_centroid, h3_index}
import h3xwrapper.utils.{Spatial, getH3EdgeLength}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, lit, row_number}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Buffer, ST_Distance, ST_H3CellIDs, ST_Transform}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.{ST_Contains, ST_Intersects}

package object h3 {
  def pointInPolygonJoin(polygonsDataFrame: DataFrame
                         , polygonColName: String
                         , pointsDataFrame: DataFrame
                         , pointColName: String
                         , h3Resolution: Int = 7
                         , distanceTolerance: Double = 0.001
                        ): DataFrame = {
    val polygonsDataFrameCheckPointed: DataFrame =
      polygonsDataFrame.repartition(100)
        .localCheckpoint()
    val polygonsTransformed: DataFrame =
      polygonsDataFrameCheckPointed
        .getGeometryInH3Exploded(polygonColName, h3Resolution, distanceTolerance)
    val pointsTransformed: DataFrame = pointsDataFrame
      .getPointInH3(pointColName, h3Resolution)
    polygonsTransformed.join(pointsTransformed, Seq(h3_index))
      .drop(h3_index)
      .where(ST_Contains(polygonColName, pointColName))
  }

  def geometryInsidePolygonJoin(geometryDataFrame: DataFrame
                                , geometryColName: String
                                , polygonDataFrame: DataFrame
                                , polygonColName: String
                                , h3Resolution: Int = 7
                                , distanceTolerance: Double = 0.001): DataFrame = {
    val polygonsCheckPointed: DataFrame = polygonDataFrame.repartition(100).localCheckpoint()
    val polygonTransformed: DataFrame = polygonsCheckPointed
      .getGeometryInH3Exploded(polygonColName, h3Resolution, distanceTolerance)

    val geometryTransformed: DataFrame = geometryDataFrame
      .addGeometryCentroidColumn(geometryColName, geometry_centroid, distanceTolerance)
      .getPointInH3(geometry_centroid, h3Resolution)
      .drop(geometry_centroid)
      .localCheckpoint()
    polygonTransformed.join(geometryTransformed, Seq(h3_index))
      .drop(h3_index)
      .where(ST_Contains(polygonColName, geometryColName))
  }


  def geometriesIntersectJoin(geometryDataFrame1: DataFrame
                              , geometryColName1: String
                              , geometryIdColName1: String
                              , geometryDataFrame2: DataFrame
                              , geometryColName2: String
                              , geometryIdColName2: String
                              , h3Resolution: Int = 7
                              , distanceTolerance: Double = 0.001): DataFrame = {
    val geometryFilledWithH3: DataFrame = geometryDataFrame1
      .getGeometryInH3Exploded(geometryColName1, h3Resolution, distanceTolerance)

    val geometryH3Boundary: DataFrame = geometryDataFrame2
      .getGeometryBoundaryInH3Exploded(geometryColName2, h3Resolution, distanceTolerance)

    geometryFilledWithH3.join(geometryH3Boundary, Seq(h3_index))
      .where(ST_Intersects(geometryColName1, geometryColName2))
      .dropDuplicates(geometryIdColName1, geometryIdColName2)

  }


  def getPointsInRangeFromPoints(pointsDfSource: DataFrame
                                 , sourceGeometryCol: String
                                 , pointsDfTarget: DataFrame
                                 , targetGeometryCol: String
                                 , range: Double
                                 , h3Resolution: Int
                                 , sourceCrs: String = "epsg:4326"
                                 , targetCrs: String = "epsg:2163"
                                ): DataFrame = {
    def transformMetersToK(range: Double, h3Resolution: Int): Int =
      (math.sqrt(3) * (range - 1) / (getH3EdgeLength(h3Resolution) * 3)).toInt

    val k: Int = transformMetersToK(range, h3Resolution) + 1
    val pointsDfSourceTransformedToH3Range: DataFrame = pointsDfSource
      .getH3Range(sourceGeometryCol, h3Resolution, k)

    val pointsDfTargetTransformedToH3: DataFrame = pointsDfTarget
      .getPointInH3(targetGeometryCol, h3Resolution)

    pointsDfSourceTransformedToH3Range.join(pointsDfTargetTransformedToH3, Seq(h3_index))
      .transformCrs(sourceGeometryCol, sourceGeometryCol, targetCrs, sourceCrs)
      .transformCrs(targetGeometryCol, targetGeometryCol, targetCrs, sourceCrs)
      .where(ST_Distance(sourceGeometryCol, targetGeometryCol) <= range)
      .transformCrs(sourceGeometryCol, sourceGeometryCol, sourceCrs, targetCrs)
      .transformCrs(targetGeometryCol, targetGeometryCol, sourceCrs, targetCrs)

  }

  def getPointsInRangeFromPolygon(polygonsDfSource: DataFrame
                                  , sourceGeometryCol: String
                                  , pointsDfTarget: DataFrame
                                  , targetGeometryCol: String
                                  , range: Double
                                  , h3Resolution: Int
                                  , sourceCrs: String = "epsg:4326"
                                  , targetCrs: String = "epsg:2163"
                                  , distanceTolerance: Double = 0.001
                                 ): DataFrame = {
    val sourceGeometryColMeterName = s"meter_${sourceGeometryCol}"
    val targetGeometryColMeterName = s"meter_${targetGeometryCol}"
    val bufferColumnName = s"buffer_${sourceGeometryCol}"

    val polygonsDataFrameCheckPointed: DataFrame =
      polygonsDfSource.repartition(100)
        .localCheckpoint()

    val polygonsTransformedWithBuffer: DataFrame =
      polygonsDataFrameCheckPointed
        .getGeometrySimplified(sourceGeometryCol, distanceTolerance)
        .transformCrs(sourceGeometryColMeterName, sourceGeometryCol, targetCrs, sourceCrs)
        .withColumn(bufferColumnName, ST_Buffer(sourceGeometryColMeterName, range))
        .transformCrs(bufferColumnName, bufferColumnName, sourceCrs, targetCrs)
        .withColumn(h3_index, explode(ST_H3CellIDs(bufferColumnName, h3Resolution, true)))
        .drop(bufferColumnName)


    val pointsTransformed: DataFrame = pointsDfTarget
      .getPointInH3(targetGeometryCol, h3Resolution)
      .transformCrs(targetGeometryColMeterName, targetGeometryCol, targetCrs, sourceCrs)

    polygonsTransformedWithBuffer.join(pointsTransformed, Seq(h3_index))
      .where(ST_Distance(sourceGeometryColMeterName, targetGeometryColMeterName) <= range)
      .drop(sourceGeometryColMeterName, targetGeometryColMeterName)
  }

  def getPolygonsInRangeFromPolygons(polygonsDfSource: DataFrame
                                     , sourceGeometryCol: String
                                     , polygonsDfTarget: DataFrame
                                     , targetGeometryCol: String
                                     , range: Double
                                     , h3Resolution: Int
                                     , sourceCrs: String = "epsg:4326"
                                     , targetCrs: String = "epsg:2163"
                                     , distanceTolerance: Double = 0.001): DataFrame = {
    val sourceGeometryColMeterName = s"meter_${sourceGeometryCol}"
    val targetGeometryColMeterName = s"meter_${targetGeometryCol}"
    val bufferColumnName = s"buffer_${sourceGeometryCol}"


    val polygonsDataFrameCheckPointed: DataFrame =
      polygonsDfSource.repartition(100)
        .localCheckpoint()

    val polygonsTransformedWithBuffer: DataFrame =
      polygonsDataFrameCheckPointed
        .getGeometrySimplified(sourceGeometryCol, distanceTolerance)
        .transformCrs(sourceGeometryColMeterName, sourceGeometryCol, targetCrs, sourceCrs)
        .withColumn(bufferColumnName, ST_Buffer(sourceGeometryColMeterName, range*1.5) )
        .transformCrs(bufferColumnName,bufferColumnName,sourceCrs,targetCrs)
        .withColumn(h3_index, explode(ST_H3CellIDs(bufferColumnName, h3Resolution, true)))
        .drop(bufferColumnName)


    val targetPolygonsTransformed: DataFrame = polygonsDfTarget.getGeometrySimplified(targetGeometryCol, distanceTolerance)
      .addGeometryCentroidColumn(targetGeometryCol, geometry_centroid, distanceTolerance)
      .getPointInH3(geometry_centroid, h3Resolution)
      .drop(geometry_centroid)


    polygonsTransformedWithBuffer
      .join(targetPolygonsTransformed.transformCrs(targetGeometryColMeterName, targetGeometryCol, targetCrs, sourceCrs), Seq(h3_index))
      .where(ST_Distance(sourceGeometryColMeterName, targetGeometryColMeterName) <= range)
      .drop(sourceGeometryColMeterName, targetGeometryColMeterName)
  }
}
