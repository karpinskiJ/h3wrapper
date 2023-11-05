package h3xwrapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sedona_sql.expressions.st_functions.ST_H3CellIDs
import h3xwrapper.Constants.h3_index
package object preprocessing {
  implicit class H3preprocessing(df:DataFrame){
    def geometryToH3(colName:String, h3Resolution:Int): DataFrame = {
      df.withColumn(h3_index,ST_H3CellIDs(colName,h3Resolution,false))
    }

  }
}
