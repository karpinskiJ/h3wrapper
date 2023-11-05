package h3xwrapper

import com.uber.h3core.util.LatLng
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf
import scala.jdk.CollectionConverters.asScalaBufferConverter


package object h3 {
  def getH3Shape(h3Index: Column): Column = {
    val getH3ShapeUdf = udf { (h3Index: Long) => {
      val shape = H3.instance.cellToBoundary(h3Index)
      val coordinates: List[String] = shape.asScala.toList.map(point => s"${point.lng  } ${point.lat}")
      val polygonString: String = {
        coordinates ++ coordinates.take(1)
      }.toString.drop(4)
      s"POLYGON(${polygonString})".stripMargin
    }
    }
    getH3ShapeUdf(h3Index)
  }

}
