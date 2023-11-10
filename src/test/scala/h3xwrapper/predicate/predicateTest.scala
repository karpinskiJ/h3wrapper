package h3xwrapper.predicate


import com.uber.h3core.util
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import h3xwrapper.utils.Spatial
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Buffer, ST_Distance, ST_H3CellDistance, ST_H3KRing}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Intersects
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Contains

import scala.jdk.CollectionConverters.asScalaBufferConverter

class predicateTest extends FlatSpec with BeforeAndAfter {

  implicit val spark = SparkSession.builder()
    .appName("H3XWrapper TEST")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "4g")
    .config("spark.master", "local[*]")
    .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
    .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  SedonaContext.create(spark)

  val poiPoints = spark.read.parquet("data/POI_TEST")
  val zipcodeShapes = spark.read.parquet("data/ZIP_CODE_SHAPES_TEST")
  val blockGroupShapes = spark.read.parquet("data/BLOCK_GROUP_SHAPES_TEST")

  def transformResultsToList(df: DataFrame, colName1: String, colName2: String): List[(String, String)] =
    df.select(colName1, colName2)
      .collect().toList.map(x => (x.getAs[String](colName1), x.getAs[String](colName2))).sorted

  "polygonContainsPointJoin" should "map points which belong to polygon if datasets do not contain h3 indexes" in {
    val h3Result = polygonContainsPointJoin(zipcodeShapes
      , "zc_id"
      , "zc_shape"
      , poiPoints
      , "point_id"
      , "point"
      , Some(10)
    )
    val sedonaResult = zipcodeShapes.join(poiPoints, ST_Contains("zc_shape", "point"))
    assertResult(transformResultsToList(sedonaResult, "zc_id", "point_id"))(transformResultsToList(h3Result, "zc_id", "point_id"))
  }
  "polygonContainsPointJoin" should "map points which belong to polygon if datasets contain h3_index" in {
    val h3Result = polygonContainsPointJoin(zipcodeShapes.getPolygonInH3("zc_shape", 10)
      , "zc_id"
      , "zc_shape"
      , poiPoints.getPointInH3("point", 10)
      , "point_id"
      , "point"
    )
    val sedonaResult = zipcodeShapes.join(poiPoints, ST_Contains("zc_shape", "point"))
    assertResult(transformResultsToList(sedonaResult, "zc_id", "point_id"))(transformResultsToList(h3Result, "zc_id", "point_id"))
  }

  "polygonContainsPolygonJoin" should "map polygons which are included in other polygons" in {
    val h3Result = polygonContainsPolygonJoin(zipcodeShapes
      , "zc_id"
      , "zc_shape"
      , blockGroupShapes
      , "bg_id"
      , "bg_shape"
      , Some(11)
    )

    val sedonaResult = zipcodeShapes.join(blockGroupShapes, ST_Contains("zc_shape", "bg_shape"))
    assertResult(transformResultsToList(sedonaResult, "zc_id", "bg_id"))(transformResultsToList(h3Result, "zc_id", "bg_id"))
  }

  "polygonContainsPolygonJoin" should "map polygons which are included in other polygons if input datasets contain h3 indexes" in {
    val h3Result = polygonContainsPolygonJoin(zipcodeShapes.getPolygonInH3("zc_shape", 11)
      , "zc_id"
      , "zc_shape"
      , blockGroupShapes.getGeometryBoundaryInH3("bg_shape", 11)
      , "bg_id"
      , "bg_shape"
    )

    val sedonaResult = zipcodeShapes.join(blockGroupShapes, ST_Contains("zc_shape", "bg_shape"))
    assertResult(transformResultsToList(sedonaResult, "zc_id", "bg_id"))(transformResultsToList(h3Result, "zc_id", "bg_id"))
  }

  "polygonIntersectsPolygon" should "map polygons which intersects each other" in {
    val h3Result = polygonIntersectsPolygon(zipcodeShapes
      , "zc_id"
      , "zc_shape"
      , blockGroupShapes
      , "bg_id"
      , "bg_shape"
        ,Some(11)
    )
    val sedonaResult = zipcodeShapes.join(blockGroupShapes, ST_Intersects("zc_shape", "bg_shape"))
    assertResult(transformResultsToList(sedonaResult, "zc_id", "bg_id"))(transformResultsToList(h3Result, "zc_id", "bg_id"))
  }

  "getPointsInRangeFromPoints" should "return points in defined range from other points" in {
    val pointsSource: DataFrame = poiPoints
    val pointsTarget: DataFrame = poiPoints.select(col("point").as("point2"), col("point_id").as("point_id2"))

    val h3Result = getPointsInRangeFromPoints(poiPoints
      , "point_id"
      , "point"
      , pointsTarget
      , "point_id2"
      , "point2"
      , 1000.0
      , 10
    )

    val sedonaResult = pointsSource.join(pointsTarget).where(ST_Distance("point", "point2") < 0.01)
    assertResult(transformResultsToList(sedonaResult, "point_id", "point_id2"))(transformResultsToList(h3Result, "point_id", "point_id2"))
  }

}
