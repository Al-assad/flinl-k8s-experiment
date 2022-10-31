package potamoi.slf4j

import org.slf4j.impl.ZIOLoggerFactory
import zio.{ZIO, ZLayer}

object ZIOSlf4jBridge {

  def initialize: ZLayer[Any, Nothing, Unit] = ZLayer {
    ZIO.runtime[Any].flatMap { runtime =>
      ZIO.succeed(ZIOLoggerFactory.initialize(runtime))
    }
  }

}
