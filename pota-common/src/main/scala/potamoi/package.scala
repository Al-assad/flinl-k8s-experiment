import potamoi.common.{FutureExtension, NumExtension, PathTool, SttpExtension, Syntax, TimeExtension, ZIOExtension}

import scala.language.implicitConversions

package object potamoi {

  val syntax  = Syntax
  val ziox    = ZIOExtension
  val sttpx   = SttpExtension
  val timex   = TimeExtension
  val futurex = FutureExtension
  val pathx   = PathTool
  val numx    = NumExtension

  def curTs: Long = System.currentTimeMillis

}
