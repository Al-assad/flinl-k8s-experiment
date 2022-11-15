package potamoi.conf

import org.slf4j.LoggerFactory
import potamoi.common.TestActor
import potamoi.logger.LogsLevel.{DEBUG, INFO}
import potamoi.logger.LogsStyle
import potamoi.logger.PotaLogger.layer
import potamoi.testkit.STActorSpec
import zio.{ZIO, ZIOAspect}

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

  "PotaLogger" should {

    "normal" in {
      ef.provide(layer(level = DEBUG)).debug.runSpec
    }
    "with annotation" in {
      ef2.provide(layer(level = INFO, style = LogsStyle.Plain)).runSpec
    }
  }

}
