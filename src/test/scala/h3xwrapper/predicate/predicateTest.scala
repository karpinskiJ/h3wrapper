package h3xwrapper.predicate



import h3xwrapper.experiments.runExperiments
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.functions.{avg, col, expr, lit}


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


  "pointInPolygonJoin" should "map records, if polygon contains point" in {
    val h3Result = h3.pointInPolygonJoin(zipcodeShapes, "zc_shape", poiPoints, "point", 7)
    val sedonaResult = sedona.pointInPolygonJoin(zipcodeShapes, "zc_shape", poiPoints, "point")
    h3Result.explain()
    sedonaResult.explain()
    assertResult(sedonaResult.count())(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "zc_id", "point_id"))(transformResultsToList(h3Result, "zc_id", "point_id"))
  }

  "geometryInsidePolygonJoin" should "map records if geometry is inside polygon" in {
    val h3Result = h3.geometryInsidePolygonJoin(blockGroupShapes
      , "bg_shape"
      , zipcodeShapes
      , "zc_shape"
      , 7)

    val sedonaResult = sedona.geometryInsidePolygonJoin(blockGroupShapes
      , "bg_shape"
      , zipcodeShapes
      , "zc_shape")

    h3Result.explain()
    sedonaResult.explain()
    assertResult(sedonaResult.count())(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "zc_id", "bg_id"))(transformResultsToList(h3Result, "zc_id", "bg_id"))
  }

  "geometriesIntersectJoin" should "map polygons which intersects each other" in {
    val h3Result = h3.geometriesIntersectJoin(zipcodeShapes
      , "zc_shape"
      , "zc_id"
      , blockGroupShapes
      , "bg_shape"
      , "bg_id"
      , 7)

    val sedonaResult = sedona.geometriesIntersectJoin(zipcodeShapes
      , "zc_shape"
      , blockGroupShapes
      , "bg_shape")

    assertResult(sedonaResult.count())(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "zc_id", "bg_id"))(transformResultsToList(h3Result, "zc_id", "bg_id"))
  }


  "getPointsInRangeFromPoints" should "return points in defined range from other points" in {
    val pointsSource: DataFrame = poiPoints
    val pointsTarget: DataFrame = poiPoints.select(col("point").as("point2"), col("point_id").as("point_id2"))

    val h3Result = h3.getPointsInRangeFromPoints(poiPoints, "point", pointsTarget, "point2", 10000.0, 7)
    val sedonaResult = sedona.sedonaGetPointsInRangeFromPoints(pointsSource, "point", pointsTarget, "point2", 10000)

    assertResult(sedonaResult.count())(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "point_id", "point_id2"))(transformResultsToList(h3Result, "point_id", "point_id2"))
  }


  "getPointsInRangeFromPolygon" should "return points in defined range from polygon" in {
    val h3Result = h3.getPointsInRangeFromPolygon(zipcodeShapes
      , "zc_shape"
      , poiPoints
      , "point"
      , 10000.0
      , 7
    )

    val sedonaResult =
      sedona.sedonaGetGeometryInRangeFromPolygon(
        zipcodeShapes
        , "zc_shape"
        , poiPoints
        , "point"
        , 10000.0)


    assertResult(sedonaResult.count)(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "point_id", "zc_id"))(transformResultsToList(h3Result, "point_id", "zc_id"))
  }
  "getPolygonsInRangeFromPolygon" should "return points in defined range from polygon" in {

    val h3Result = h3.getPolygonsInRangeFromPolygons(zipcodeShapes
      , "zc_shape"
      , blockGroupShapes
      , "bg_shape"
      , 10000
      , 7)

    val sedonaResult =
      sedona.sedonaGetGeometryInRangeFromPolygon(zipcodeShapes
        , "zc_shape"
        , blockGroupShapes
        , "bg_shape"
        , 10000)

    assertResult(sedonaResult.count())(h3Result.count())
    assertResult(transformResultsToList(sedonaResult, "bg_id", "zc_id"))(transformResultsToList(h3Result, "bg_id", "zc_id"))
  }


}
