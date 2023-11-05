package h3xwrapper.predicate

import breeze.linalg.min
import com.uber.h3core.util
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import h3xwrapper.h3.{H3, getH3Shape}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, expr, lit, posexplode, row_number}
//import org.apache.spark.sql.sedona_sql.expressions.ST_Y
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Area, ST_AsText, ST_Centroid, ST_FlipCoordinates, ST_H3CellIDs, ST_SimplifyPreserveTopology, ST_Transform, ST_X,ST_Y,ST_Boundary}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Contains
import com.uber.h3core.H3Core
import com.uber.h3core.util.LatLng
import com.uber.h3core.LengthUnit

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


  "ss" should "" in {
    val bgShapes = spark.read.parquet("data/test/BG_SHAPES")
      .withColumn("h3_cells",ST_H3CellIDs("bg_shape",7,true))
      .where(col("row_number")===1)
      .withColumn("h3_cells",explode(col("h3_cells")))
      .withColumn("h3_shape",getH3Shape(col("h3_cells")))
      .withColumn("bg_shape_wkt",ST_AsText(col("bg_shape")))
    bgShapes.select("h3_shape").show(false)
    bgShapes.write.mode("overwrite").parquet("data/test/FILL_FULL_COVERAGE")
    spark.read.parquet("data/test/BG_SHAPES")
      .withColumn("h3_cells", ST_H3CellIDs("bg_shape", 15, false))
      .where(col("row_number") === 1)
      .withColumn("h3_cells", explode(col("h3_cells")))
      .withColumn("h3_shape", getH3Shape(col("h3_cells")))
      .withColumn("bg_shape_wkt", ST_AsText(col("bg_shape")))
      .write.mode("overwrite"). parquet("data/test/FILL")




//    bgShapes.select("h3_cells").show(false)




//    STContains(blockGroupShapes,"bg_shape",zipCodeShapes,"zc_shape")
  }

  "distance" should "sss" in {
    val point1 = new LatLng(52.23950118578871,21.076400473543977)
    val point2 = new LatLng(52.23248721956916,21.073722398108334)

    println(H3Core.newInstance()
      .greatCircleDistance(point1,point2,LengthUnit.m

  }


  "ssa" should "hhh" in {
    import spark.implicits._
    val df = Seq((List(1,2),"2")).toDF("id","id2")
    println(df.schema.toList.filter(_.name=="id").head.dataType)
//    df.printSchema()
    val bgShapes = spark.read.parquet("data/BLOCK_GROUP")
    val zcShapes = spark
      .read.parquet("data/ZIPCODE")
      .withColumn("zc_shape",ST_SimplifyPreserveTopology("zc_shape",0.01))
      .withColumn("boundary",ST_Boundary("zc_shape"))
      .withColumn("h3_boundary",ST_H3CellIDs("boundary",10,false))
      .where((col("zc_id")==="35592"))
      .withColumn("h3_boundary",explode(col("h3_boundary")))
      .withColumn("h3_boundary_shape",getH3Shape(col("h3_boundary")))
      .withColumn("zc_shape_wkt",ST_AsText("zc_shape"))
    zcShapes.select("h3_boundary_shape","zc_shape","zc_id","zc_shape_wkt").write.mode("overwrite").parquet("data/POLYGON_BOUNDARY")



//
//    val bgInH3 =
//    bgShapes
////      .withColumn("centroid",ST_Centroid("bg_shape"))
////      .withColumn("long",ST_X("centroid"))
////      .withColumn("lat",ST_Y("centroid"))
//      .withColumn("h3_index",ST_H3CellIDs("bg_shape",10,false))
////      .select("h3_index_point","h3_index_polygon")
//
//   val  zcInH3 = zcShapes      .withColumn("h3_index",ST_H3CellIDs("zc_shape",10,false))
//
//    containsJoin(bgInH3,zcInH3).count()
//    println(bgInH3.schema.toList.map(x=>x.dataType))


    ////    bgInH3.write.mode("overwrite")
//    spark.read
//      .parquet("data/BG_IN_H3_9")
//
//    val zcInH3 =
////      zcShapes.withColumn("h3_shape",ST_H3CellIDs("zc_shape",7,false))
////        .withColumn("h3_shape", explode(col("h3_shape")))
////
////    zcInH3.write.mode("overwrite")
//    spark.read
//        .parquet("data/ZC_IN_H3_9")
//
//    println(bgInH3.join(zcInH3, Seq("h3_shape")).groupBy("zc_id","bg_id") .count().count())


  }


    //    val startTime1 = System.nanoTime()
//    zcShapes
//      .withColumn("zc_shape_meter",ST_FlipCoordinates(col("zc_shape")))
//      .withColumn("zc_shape_meter",expr("ST_Transform(zc_shape_meter,'epsg:4326','epsg:2163')"))
//      .withColumn("zc_area",ST_Area("zc_shape_meter"))
//      .select(functions.avg("zc_area")/1000000.toDouble,functions.max("zc_area")/1000000.toDouble)
//      .show()
//    bgShapes
//      .withColumn("bg_shape_meter", ST_FlipCoordinates(col("bg_shape")))
//      .withColumn("bg_shape_meter", expr("ST_Transform(bg_shape_meter,'epsg:4326','epsg:2163')"))
//      .withColumn("bg_area", ST_Area("bg_shape_meter"))
//      .select(functions.avg("bg_area") / 1000000.toDouble, functions.max("bg_area") / 1000000.toDouble)
//      .show()



////      .withColumn("zc_centroid",ST_Centroid("zc_shape") )
////      .withColumn("h3_index",ST_H3CellIDs("zc_centroid",7,true)(0) )
////      .withColumn("h3_shape",getH3Shape(col("h3_index") ))
////      .withColumn("zc_centroid",ST_AsText("zc_centroid"))
////      .withColumn("zc_shape",ST_AsText("zc_shape"))
////      .write.mode("overwrite").parquet("test/ZC_H3_CENTROIDS")
//
//
//    val startTime2 = System.nanoTime()
//    val result2 = zcShapes.join(bgShapes).where(ST_Contains("zc_shape","bg_shape"))
//    println(result2.count())
//    val endTime2 = System.nanoTime()
//    val executionTime2 = (endTime2 - startTime2) / 1000000.0
//
//
//    val startTime1 = System.nanoTime()
//    val result = polygonContainsOther(bgShapes,"bg_shape",zcShapes,"zc_shape")
//
//    println(result.count())
//    println(result.where(ST_Contains("zc_shape","bg_shape") ).count())
//
//
////    result.coalesce(1) .write.option("header","true").csv("data/test/debug")
//
//    val endTime1 = System.nanoTime()
//    val executionTime1 = (endTime1 - startTime1) / 1000000.0

//    println(s"H3: ${executionTime1}")
//    println(s" ${executionTime2}")



}
