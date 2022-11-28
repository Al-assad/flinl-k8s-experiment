package potamoi.config

import com.softwaremill.quicklens.ModifyPimp
import com.typesafe.config.{Config, ConfigFactory}
import potamoi.common
import potamoi.config.AkkaConf.{cborSerializableClzPath, jsonSerializableClzPath}
import potamoi.config.DDataReadLevel.ReadLocal
import potamoi.config.DDataWriteLevel.WriteMajority
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}
import zio.{IO, ZIO}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Akka system configuration.
 */
case class AkkaConf(
    @name("sys-name") systemName: String = "potamoi",
    @name("host") host: Option[String] = None,
    @name("port") port: Int = 3300,
    @name("seeds-address") seedsAddress: Set[String] = Set.empty,
    @name("default-ask-timeout") defaultAskTimeout: Duration = 5.seconds,
    @name("loglevel") logLevel: String = "INFO",
    @name("over-node-roles") overNodeRoles: Set[String] = Set.empty,
    @name("seeds-join-tolerance") seedsJoinTolerance: Duration = 45.seconds,
    @name("ddata") ddata: DDataConfs = DDataConfs(),
    @name("ext") extRawAkkaConfig: Option[String] = None)
    extends Resolvable {

  override def resolve: PotaConf => PotaConf = { root =>
    root
      .modify(_.akka.overNodeRoles)
      .setTo(root.nodeRoles.map(_.toString) ++ overNodeRoles)
      .modify(_.akka.logLevel)
      .setTo(root.log.level.toString)
  }

  def toAkkaRawConfig: IO[Throwable, Config] = ZIO.attempt {
    val seedNodeConfs = seedsAddress.map(a => s""""akka://$systemName@$a"""").mkString(",")
    val rawConf =
      s"""akka.loglevel = $logLevel
         |akka.actor {
         | provider = cluster
         | serializers {
         |   jackson-cbor = "akka.serialization.jackson.JacksonCborSerializer"
         |   jackson-json = "akka.serialization.jackson.JacksonJsonSerializer"
         | }
         | serialization-bindings {
         |   "$cborSerializableClzPath" = jackson-cbor
         |   "$jsonSerializableClzPath" = jackson-json
         | }
         |}
         |akka.remote.artery.canonical {
         |  port = $port
         |  ${if (host.isDefined) s"hostname = ${host.get}" else ""}
         |}
         |akka.cluster {
         |  roles = [${overNodeRoles.mkString(",")}]
         |  ${if (seedNodeConfs.nonEmpty) s"seed-nodes = [$seedNodeConfs]" else ""}
         |  downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
         |  shutdown-after-unsuccessful-join-seed-nodes = ${seedsJoinTolerance.toString}
         |}
         |akka.cluster.sharding.remember-entities-store = ddata
         |coordinated-shutdown.exit-jvm = on
         |""".stripMargin
    val conf = ConfigFactory.parseString(rawConf)
    extRawAkkaConfig match {
      case None      => conf
      case Some(ext) => ConfigFactory.parseString(ext).withFallback(conf)
    }
  }
}

object AkkaConf {
  private val cborSerializableClzPath = "potamoi.cluster.CborSerializable"
  private val jsonSerializableClzPath = "potamoi.cluster.JsonSerializable"

  implicit val durCodec: JsonCodec[Duration] = common.scalaDurationCodec
  implicit val codec: JsonCodec[AkkaConf]    = DeriveJsonCodec.gen[AkkaConf]
}

/**
 * Akka distributed data configs.
 */
case class DDataConfs(
    @name("default") default: DDataConf = DDataConf(),
    @name("flink-cluster-id") flinkClusterIds: Option[DDataConf] = None,
    @name("flink-jobs-index") flinkJobIndex: Option[DDataConf] = None,
    @name("flink-cluster-index") flinkClusterIdx: Option[DDataConf] = None,
    @name("flink-rest-endpoint") flinkRestEndpoint: Option[DDataConf] = None) {

  def getFlinkClusterIds: DDataConf   = flinkClusterIds.getOrElse(default)
  def getFlinkJobIndex: DDataConf     = flinkJobIndex.getOrElse(default)
  def getFlinkClusterIndex: DDataConf = flinkClusterIdx.getOrElse(default)
  def getFlinkRestEndpoint: DDataConf = flinkRestEndpoint.getOrElse(default)
}

object DDataConfs {
  implicit val codec: JsonCodec[DDataConfs] = DeriveJsonCodec.gen[DDataConfs]
}

/**
 * Akka distributed data configurations.
 */
case class DDataConf(
    @name("ask-timeout") askTimeout: Duration = 5.seconds,
    @name("write-level") writeLevel: DDataWriteLevel = WriteMajority(10.seconds, 0),
    @name("read-level") readLevel: DDataReadLevel = ReadLocal)

object DDataConf {
  implicit val durCodec: JsonCodec[Duration] = common.scalaDurationCodec
  implicit val codec: JsonCodec[DDataConf]   = DeriveJsonCodec.gen[DDataConf]
}

/**
 * Akka distributed data write level.
 */
sealed trait DDataWriteLevel

object DDataWriteLevel {
  @name("local") case object WriteLocal                                                                            extends DDataWriteLevel
  @name("majority") case class WriteMajority(timeout: Duration, additional: Int, @name("min-cap") minCap: Int = 0) extends DDataWriteLevel
  @name("all") case class WriteAll(timeout: Duration)                                                              extends DDataWriteLevel

  implicit val durCodec: JsonCodec[Duration]     = common.scalaDurationCodec
  implicit val codec: JsonCodec[DDataWriteLevel] = DeriveJsonCodec.gen[DDataWriteLevel]
}

/**
 * Akka distributed data read level.
 */
sealed trait DDataReadLevel

object DDataReadLevel {
  @name("local") case object ReadLocal                                                                            extends DDataReadLevel
  @name("majority") case class ReadMajority(timeout: Duration, additional: Int, @name("min-cap") minCap: Int = 0) extends DDataReadLevel
  @name("all") case class ReadAll(timeout: Duration)                                                              extends DDataReadLevel

  implicit val durCodec: JsonCodec[Duration]    = common.scalaDurationCodec
  implicit val codec: JsonCodec[DDataReadLevel] = DeriveJsonCodec.gen[DDataReadLevel]
}
