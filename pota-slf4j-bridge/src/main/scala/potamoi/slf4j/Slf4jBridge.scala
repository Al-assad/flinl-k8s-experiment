package potamoi.slf4j

import org.slf4j.impl.ZIOLoggerFactory
import zio.{ZIO, ZLayer}

/**
 * Bridge for translating SLF4J logging to ZIO logging.
 */
object Slf4jBridge {

  def initialize: ZLayer[Any, Nothing, Unit] = ZLayer {
    ZIO.runtime[Any].flatMap { runtime =>
      ZIO.succeed(ZIOLoggerFactory.initialize(runtime, Vector.empty))
    }
  }

  def initialize(mdcKeys: Vector[String]): ZLayer[Any, Nothing, Unit] = ZLayer {
    ZIO.runtime[Any].flatMap { runtime =>
      ZIO.succeed(ZIOLoggerFactory.initialize(runtime, mdcKeys))
    }
  }

}
