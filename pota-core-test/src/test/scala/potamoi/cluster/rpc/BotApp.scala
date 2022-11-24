package potamoi.cluster.rpc

import com.softwaremill.quicklens.ModifyPimp
import potamoi.cluster.PotaActorSystem
import potamoi.logger.PotaLogger
import potamoi.syntax._
import potamoi.testkit.PotaDev
import potamoi.timex._
import zio.Schedule.spaced
import zio.{ZIO, ZIOAppDefault}

import scala.concurrent.duration.DurationInt

/**
 * Rpc server app.
 */
object BotServerApp extends ZIOAppDefault {

  val rpcServers = BotRpcServer.init *> ZIO.never

  val run = rpcServers.provide(
    PotaDev.conf.change(
      _ modify (_.akka) using (_.copy(
        host = "127.0.0.1",
        port = 2551,
        seedsAddress = Set("127.0.0.1:2551")
      ))
    ),
    PotaLogger.live,
    PotaActorSystem.live,
    BotImpl.live,
  )
}

/**
 * Rpc client app.
 */
object BotClientApp extends ZIOAppDefault {
  val run = ZIO
    .service[Bot]
    .flatMap { bot =>
      bot.greet("hi").debug.ignore.repeat(spaced(1.seconds))
    }
    .provide(
      PotaDev.conf.change(
        _ modify (_.akka) using (_.copy(
          host = "127.0.0.1",
          port = 2552,
          seedsAddress = Set("127.0.0.1:2551")
        ))
      ),
      PotaLogger.live,
      PotaActorSystem.live,
      BotRemote.live
    )
}
