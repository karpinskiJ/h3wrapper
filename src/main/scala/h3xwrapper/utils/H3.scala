package h3xwrapper.utils

import com.uber.h3core.H3Core

object H3 extends Serializable {
  val instance  = H3Core.newInstance()

}
