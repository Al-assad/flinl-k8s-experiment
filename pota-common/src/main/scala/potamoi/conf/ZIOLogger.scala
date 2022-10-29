package potamoi.conf

import zio.{ZIO, ZIOAppDefault}
import zio.logging.backend.SLF4J
import zio.logging.{LogFormat, console}

object ZIOLogger {

  val logger1 = zio.Runtime.removeDefaultLoggers >>> console(LogFormat.colored)
  val logger2 = zio.Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val logger = console(LogFormat.colored) >>> SLF4J.slf4j

}


object Foo {
  val ef = ZIO.logInfo("hello") *>
           ZIO.logInfo("hi") *>
           ZIO.logError("Boom!")
}


object test extends ZIOAppDefault {

  val logger1 = zio.Runtime.removeDefaultLoggers >>> console(LogFormat.colored)
  val logger2 = zio.Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val logger = console(LogFormat.colored) >>> SLF4J.slf4j

  val run = Foo.ef.provide(logger2)
}
