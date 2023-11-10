//package h3xwrapper.predicate
//
//class prepareTestData {
//
//}
//
//"readPoi" should "" in {
//  val w = Window.orderBy("point")
//  val zcTestData = spark.read.parquet("data/ZIP_CODE_SHAPES_TEST")
//  val poi = sedona.loadShapefileToDf("data/alabama_poi")
//    .withColumnRenamed("geometry", "point")
//    .select("point")
//  //    .withColumn("point_id",row_number().over(w))
//
//
//  val predicatePointsInDistanceOfPoints = poi.join(poi.select(col("point").as("point_2"))).where(ST_Distance("point", "point_2") < 0.1).limit(5)
//    .select("point")
//
//
//  //
//  val predicatePointsInPolygon =
//    zcTestData.join(poi, ST_Contains("zc_shape", "point")).select("point").limit(5)
//
//
//  val predicatePointsInDistanceOfPolygon = zcTestData.join(poi.join(predicatePointsInPolygon, Seq("point"), "left_anti"))
//    .where(ST_Distance("zc_shape", "point") < 0.1 && ST_Distance("zc_shape", "point") > 0.05)
//    .limit(5)
//    .select("point")
//
//  val predicatePointsOutOfScope = poi.join(zcTestData).where(ST_Distance("zc_shape", "point") > 1).limit(5).select("point")
//
//
//  val overallPointData = predicatePointsInDistanceOfPoints.unionByName(predicatePointsInPolygon).unionByName(predicatePointsInDistanceOfPolygon).unionByName(predicatePointsOutOfScope)
//
//  overallPointData.withColumn("point_id", row_number().over(w)).distinct()
//    .withColumn("point_id", col("point_id").cast("string"))
//    .withColumn("lat", ST_Y("point"))
//    .withColumn("long", ST_X("point"))
//    .withColumn("point_wkt", ST_AsText("point"))
//    .write.mode("overwrite")
//
//    .parquet("data/POI_TEST")
//
//}
//
//"prepareTest" should "" in {
//  val blockGroup = spark.read.parquet("C:\\projects\\h3xwrapper\\data\\BLOCK_GROUP")
//  blockGroup.printSchema()
//  val zipcodes = spark.read.parquet("C:\\projects\\h3xwrapper\\data\\ZIPCODE")
//  zipcodes.printSchema()
//  val containsPredicateDataset =
//    zipcodes
//      .join(blockGroup, ST_Contains("zc_shape", "bg_shape"))
//      .limit(5)
//  //        .select("zc_id", "bg_id")
//
//  val intersectsPredicate = zipcodes.join(blockGroup, ST_Intersects("bg_shape", "zc_shape"))
//    .join(containsPredicateDataset, Seq("bg_id", "zc_id"), "left_anti")
//    .limit(5)
//
//  val distancePredicate = zipcodes.join(blockGroup).where(ST_Distance("bg_shape", "zc_shape") < 0.1 && ST_Distance("bg_shape", "zc_shape") > 0.05).limit(5)
//
//  val notConnectedPredicate = zipcodes.join(blockGroup, !ST_Intersects("bg_shape", "zc_shape"))
//    .join(containsPredicateDataset, Seq("bg_id", "zc_id"), "left_anti")
//    .join(intersectsPredicate, Seq("bg_id", "zc_id"), "left_anti")
//    .join(distancePredicate, Seq("bg_id", "zc_id"), "left_anti")
//    .limit(5)
//
//
//  val overallDataset = containsPredicateDataset.unionByName(intersectsPredicate).unionByName(notConnectedPredicate).unionByName(distancePredicate)
//
//  val bgTestData = overallDataset.select("bg_id", "bg_shape").distinct()
//  bgTestData.withColumn("bg_shape_wkt", ST_AsText("bg_shape")).coalesce(1)
//    .write.mode("overwrite").parquet("data/BLOCK_GROUP_SHAPES_TEST")
//
//  val zcTestData = overallDataset.select("zc_id", "zc_shape").distinct()
//
//  zcTestData.withColumn("zc_shape_wkt", ST_AsText("zc_shape")).coalesce(1)
//    .write.mode("overwrite").parquet("data/ZIP_CODE_SHAPES_TEST")
//
//
//}