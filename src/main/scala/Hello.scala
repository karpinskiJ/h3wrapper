import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import h3xwrapper.sedona.loadShapefileToDf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.spark.sql.sedona_sql.expressions.{ST_AsText, ST_Intersects, ST_Point, st_functions,ST_H3CellIDs}


object Hello extends App {


  implicit val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
    .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  SedonaContext.create(spark)

    val blockGroups = loadShapefileToDf("data/tl_2020_01_bg")
     .select(col("geometry").as("bg_shape"),col("GEOID").as("bg_id"))
      .coalesce(5)
  blockGroups.show()
//
//
//
//    val zipCodes = loadShapefileToDf("data/tl_2020_us_zcta520")
//      .where(col("ZCTA5CE20").between("35004", "36925"))
//      .select(col("geometry").as("zc_shape"),col("ZCTA5CE20").as("zc_id"))
//      .coalesce(5)
//
//
//    blockGroups.write.mode("overwrite").parquet("data/BLOCK_GROUP")
//    zipCodes.write.mode("overwrite").parquet("data/ZIPCODE")
////  val blockGroups = spark.read.parquet("data/BLOCK_GROUP")
////  val zipCodes = spark.read.parquet("data/ZIPCODE")
//
//
//  blockGroups.join(zipCodes).where(expr("ST_Intersects(bg_shape,zc_shape)"))
//    .write.mode("overwrite")parquet("data/INTERSECTED")
//  val options = Map("delimiter"->",","header"->"true")
//  spark.read.parquet("data/INTERSECTED")
//    .withColumn("bg_shape",expr("ST_SimplifyPreserveTopology(bg_shape,0.001)"))
//    .withColumn("zc_shape",expr("ST_SimplifyPreserveTopology(zc_shape,0.001)"))
//    .withColumn("wkt_bg",expr("ST_AsText(bg_shape)"))
//    .withColumn("wkt_zc",expr("ST_AsText(zc_shape)"))
//    .drop("bg_shape","zc_shape")
//    .coalesce(1)
//    .write.mode("overwrite").options(options).csv("data/INTERSECTED_WKT")
//
////
//
//






  //35004 to 36925
}
