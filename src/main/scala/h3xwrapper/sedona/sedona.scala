package h3xwrapper

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Geometry

package object sedona {
  def loadShapefileToDf(path: String)(implicit spark: SparkSession): DataFrame = {
    val rdd: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
    Adapter.toDf(rdd, spark)
  }

}

