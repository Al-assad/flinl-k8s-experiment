package potamoi.conf

import org.slf4j.LoggerFactory
import potamoi.common.TestActor
import potamoi.conf.LogsLevel.{DEBUG, INFO}
import potamoi.conf.PotaLogger.logLayer
import potamoi.testkit.STActorSpec
import zio.{ZIO, ZIOAspect}

import scala.collection.JavaConverters._

class PotaLoggerSpec extends STActorSpec {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val ef = {
    ZIO.logDebug("debug-msg") *>
    ZIO.logInfo("info-msg") *>
    ZIO.logWarning("warning-msg") *>
    ZIO.logError("fail-msg") *>
    ZIO.succeed(logger.debug("slf4j-debug-msg")) *>
    ZIO.succeed(logger.info("slf4j-info-msg")) *>
    ZIO.succeed(logger.warn("slf4j-warn-msg")) *> (
      for {
        actor <- ZIO.attempt(actorKit.spawn(TestActor()))
        _     <- ZIO.attempt(actor ! TestActor.Touch)
      } yield ()
    )
  }

  private val ef2 = {
    ZIO.logInfo("msg1") *>
    ZIO.logInfo("msg2") @@ ZIOAspect.annotated("k1", "v1") *>
    ZIO.logInfo("msg3")
  }

  "PotaLogger" ignore {

    "normal" in {
      ef.provide(logLayer(level = DEBUG)).debug.run
    }
    "with annotation" in {
      ef2.provide(logLayer(level = INFO, style = LogsStyle.Plain)).run
    }
  }

}
