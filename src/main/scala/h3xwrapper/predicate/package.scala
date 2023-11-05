package h3xwrapper

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array_contains, col, concat, datediff, explode, udf}
import org.apache.spark.sql.sedona_sql.expressions.st_functions.{ST_Centroid, ST_GeometryType, ST_H3CellIDs}
import org.apache.spark.sql.sedona_sql.expressions.st_predicates.ST_Contains
import org.locationtech.jts.geom.Geometry
import h3xwrapper.Constants.h3_index

package object predicate {
  def intersectsJoin(innerObjectDf: DataFrame, innerObjectColId: String,
                   outerObjectDf: DataFrame, outerObjectColId: String
                  ): DataFrame = {
    val innerObjectDfExploded = innerObjectDf
      .withColumn(h3_index, explode(col(h3_index)))
    val outerObjectDfExploded = outerObjectDf
      .withColumn(h3_index, explode(col(h3_index)))
    outerObjectDfExploded
      .join(innerObjectDfExploded, Seq(h3_index))
      .dropDuplicates(innerObjectColId, outerObjectColId)
  }

private  def h3ContainsPredicate(outerH3Index: Column,innerH3Index:Column): Column = {
    val h3ContainsPredicateUdf = udf { (outerH3Index:List[Long],innerH3Index:List[Long]) => {
      innerH3Index.forall(x=>outerH3Index.contains(x))
    }}
    h3ContainsPredicateUdf(outerH3Index,innerH3Index)
  }
  def containsJoin(innerObjectDf: DataFrame,
                   outerObjectDf: DataFrame
                  ): DataFrame = {
    val innerColName:String = s"inner_${h3_index}"
    val outerColName:String = s"outer_${h3_index}"

    val innerDf:DataFrame = innerObjectDf.withColumnRenamed(h3_index,innerColName)
    val outerDf:DataFrame = outerObjectDf.withColumnRenamed(h3_index,outerColName)
    innerDf.join(outerDf,h3ContainsPredicate(col(innerColName),col(outerColName)))
  }




//  private def addH3Buffer(df: DataFrame): DataFrame = {
//    df.withColumn()
//
//
//  }
//
//
//
//  def rangeJoinPoints(innerObjectDf: DataFrame, innerObjectColId: String,
//                outerObjectDf: DataFrame, outerObjectColId: String): DataFrame = {
//
//
//
//
//
//  }
//
//
//}


}
