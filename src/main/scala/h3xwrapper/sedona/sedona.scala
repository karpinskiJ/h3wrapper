package h3xwrapper

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Geometry

package object sedona {
  def loadShapefileToDf(path: String)(implicit spark: SparkSession): DataFrame = {
    val rdd: SpatialRDD[Geometry] = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
    Adapter.toDf(rdd, spark)
  }

  implicit class Spatial(df:DataFrame){
    def transformCrs(targetColName:String,sourceColName:String,targetCrs:String="epsg:2163",sourceCrs:String="epsg:4326"):DataFrame=
    df.withColumn(targetColName,expr(s"ST_Transform(ST_FlipCoordinates($sourceColName),'$sourceCrs','$targetCrs')"))
  }

}

