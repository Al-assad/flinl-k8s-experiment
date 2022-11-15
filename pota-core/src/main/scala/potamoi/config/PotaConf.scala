package potamoi.config

import com.typesafe.config.{Config, ConfigFactory}
import potamoi.config.NodeRole.NodeRole
import potamoi.syntax._
import zio.ZIO.{attempt, logInfo}
import zio.config.magnolia.{descriptor, name}
import zio.config.typesafe.TypesafeConfigSource
import zio.config.{generateDocs, read, ReadError}
import zio.json.{DeriveJsonCodec, JsonCodec}
import zio.{IO, ULayer, ZIO, ZLayer}

import java.io.File

/**
 * Potamoi root configuration.
 */
case class PotaConf(
    @name("node-roles") nodeRoles: Set[NodeRole],
    @name("local-storage-dir") localStgDir: String = "var/potamoi",
    @name("show-loaded-config") showLoadedConfig: Boolean = true,
    @name("log") log: LogConf = LogConf(),
    @name("k8s") k8s: K8sConf = K8sConf(),
    @name("s3") s3: S3Conf,
    @name("flink") flink: FlinkConf = FlinkConf(),
    @name("akka") akka: AkkaConf = AkkaConf()) {

  def resolve: PotaConf = Vector(log, k8s, s3, flink, akka).foldLeft(this)((ac, c) => c.resolve(ac))
}

object PotaConf {
  implicit val codec: JsonCodec[PotaConf] = DeriveJsonCodec.gen[PotaConf]

  /**
   * Creation from case class directly.
   */
  def layer(conf: PotaConf): ULayer[PotaConf] = ZLayer {
    ZIO.succeed(conf.resolve).tap { conf =>
      if (conf.showLoadedConfig) logInfo(s"Loaded config: \n${conf.toPrettyString}")
      else logInfo(s"Loaded config.")
    }
  }

  /**
   * Creation from Hocon Config.
   */
  def layer(hocon: IO[Throwable, Config]): ZLayer[Any, ReadError[String], PotaConf] = ZLayer {
    for {
      _ <- ZIO.unit
      source = TypesafeConfigSource.fromTypesafeConfig(hocon)
      desc   = descriptor[PotaConf] from source
      potaConf <- read(desc).map(_.resolve)
      _ <-
        if (potaConf.showLoadedConfig) logInfo(s"Loaded config from hocon: \n${potaConf.toPrettyString}")
        else logInfo(s"Loaded config from hocon.")
    } yield potaConf
  }

  /**
   * Creation from Hocon file path.
   */
  def layer(hoconFilePath: String): ZLayer[Any, ReadError[String], PotaConf] = layer(
    attempt(ConfigFactory.parseFile(new File(hoconFilePath)))
  )

  /**
   * Generates markdown doc content.
   */
  def genMdDoc: String = generateDocs(descriptor[PotaConf]).toTable.toGithubFlavouredMarkdown

}

private[config] trait Resolvable {
  def resolve: PotaConf => PotaConf = identity
}
