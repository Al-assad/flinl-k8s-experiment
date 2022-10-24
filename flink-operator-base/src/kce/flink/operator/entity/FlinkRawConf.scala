package kce.flink.operator.entity

import cats.Eval
import cats.Eval.later
import kce.common.NumExtension.{DoubleWrapper, IntWrapper}
import kce.common.{ComplexEnum, GenericPF}
import kce.conf.S3Conf
import kce.flink.operator.FlinkConfigExtension.{ConfigurationPF, EmptyConfiguration}
import kce.flink.operator.entity.CheckpointStorageType.CheckpointStorageType
import kce.flink.operator.entity.StateBackendType.StateBackendType
import org.apache.flink.configuration.Configuration
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

/**
 * Type-safe flink major configuration entries.
 */
sealed trait FlinkRawConf {

  /**
   * Dump to Flink [[Configuration]].
   */
  def toRaw: Configuration = injectRaw(EmptyConfiguration())

  /**
   * Dump configuration key-values to Map.
   */
  def toMap: Map[String, String] = toRaw.toMap.asScala.toMap

  /**
   * Evaluation behavior of Flink's raw configuration.
   */
  def rawMapping: Vector[(String, Eval[Any])]

  /**
   * Behavior of injecting configuration into Flink [[Configuration]].
   */
  def injectRaw: ConfigurationPF => ConfigurationPF = rawMapping.foldLeft(_) { case (flinkConf, (key, eval)) =>
    eval.value match {
      case None                      => flinkConf
      case Some(value)               => flinkConf.append(key, value)
      case Some(value: List[String]) => if (value.nonEmpty) flinkConf.append(key, value) else flinkConf
      case value                     => flinkConf.append(key, value)
    }
  }
}

object FlinkRawConf {
  implicit val cpuConfCodec: JsonCodec[CpuConf]               = DeriveJsonCodec.gen[CpuConf]
  implicit val memConfCodec: JsonCodec[MemConf]               = DeriveJsonCodec.gen[MemConf]
  implicit val parConfCodec: JsonCodec[ParConf]               = DeriveJsonCodec.gen[ParConf]
  implicit val webUICodec: JsonCodec[WebUIConf]               = DeriveJsonCodec.gen[WebUIConf]
  implicit val restartStgCodec: JsonCodec[RestartStgConf]     = DeriveJsonCodec.gen[RestartStgConf]
  implicit val stateBackendCodec: JsonCodec[StateBackendConf] = DeriveJsonCodec.gen[StateBackendConf]
  implicit val jmHaCodec: JsonCodec[JmHaConf]                 = DeriveJsonCodec.gen[JmHaConf]
  implicit val s3AccessConf: JsonCodec[S3AccessConf]          = DeriveJsonCodec.gen[S3AccessConf]
}

/**
 * Flink k8s cpu configuration.
 */
case class CpuConf(jm: Double = 1.0, tm: Double = -1.0, jmFactor: Double = 1.0, tmFactor: Double = 1.0) extends FlinkRawConf {
  def rawMapping = Vector(
    "kubernetes.taskmanager.cpu"              -> later(jm.ensureDoubleOr(_ > 0, 1.0)),
    "kubernetes.jobmanager.cpu.limit-factor"  -> later(jmFactor.ensureDoubleOr(_ > 0, 1.0)),
    "kubernetes.taskmanager.cpu"              -> later(tm),
    "kubernetes.taskmanager.cpu.limit-factor" -> later(tmFactor.ensureDoubleOr(_ > 0, 1.0))
  )
}

/**
 * Flink parallelism configuration.
 */
case class ParConf(numOfSlot: Int = 1, parDefault: Int = 1) extends FlinkRawConf {
  def rawMapping = Vector(
    "taskmanager.numberOfTaskSlots" -> later(numOfSlot.ensureIntMin(1)),
    "parallelism.default"           -> later(parDefault.ensureIntMin(1))
  )
}

/**
 * Flink memory configuration.
 */
case class MemConf(jmMB: Int = 1920, tmMB: Int = 1920) extends FlinkRawConf {
  def rawMapping = Vector(
    "jobmanager.memory.process.size"  -> later(jmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m")),
    "taskmanager.memory.process.size" -> later(tmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m"))
  )
}

/**
 * Flink web ui service configuration.
 */
case class WebUIConf(enableSubmit: Boolean = true, enableCancel: Boolean = true) extends FlinkRawConf {
  def rawMapping = Vector(
    "web.submit.enable" -> later(enableSubmit),
    "web.cancel.enable" -> later(enableCancel)
  )
}

/**
 * Flink task restart strategy.
 */
sealed trait RestartStgConf extends FlinkRawConf

case object NonRestartStg extends RestartStgConf {
  def rawMapping = Vector("restart-strategy" -> later("none"))
}

case class FixedDelayRestartStg(attempts: Int = 1, delaySec: Int = 1) extends RestartStgConf {
  def rawMapping = Vector(
    "restart-strategy"                      -> later("fixed-delay"),
    "restart-strategy.fixed-delay.attempts" -> later(attempts.ensureIntMin(1)),
    "restart-strategy.fixed-delay.delay"    -> later(delaySec.ensureIntMin(1).contra(e => s"$e s"))
  )
}

case class FailureRateRestartStg(delaySec: Int = 1, failureRateIntervalSec: Int = 60, maxFailuresPerInterval: Int = 1) extends RestartStgConf {
  def rawMapping = Vector(
    "restart-strategy"                                        -> later("failure-rate"),
    "restart-strategy.failure-rate.delay"                     -> later(failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s")),
    "restart-strategy.failure-rate.failure-rate-interval"     -> later(failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s")),
    "restart-strategy.failure-rate.max-failures-per-interval" -> later(maxFailuresPerInterval.ensureIntMin(1))
  )
}

/**
 * Flink state backend configuration.
 */
case class StateBackendConf(
    backendType: StateBackendType,
    checkpointStorage: CheckpointStorageType,
    checkpointDir: Option[String] = None,
    savepointDir: Option[String] = None,
    incremental: Boolean = false,
    localRecovery: Boolean = false,
    checkpointNumRetained: Int = 1)
    extends FlinkRawConf {

  def rawMapping = Vector(
    "state.backend"                  -> later(backendType.toString),
    "state.checkpoint-storage"       -> later(checkpointStorage.toString),
    "state.checkpoints.dir"          -> later(checkpointDir),
    "state.savepoints.dir"           -> later(savepointDir),
    "state.backend.incremental"      -> later(incremental),
    "state.backend.local-recovery"   -> later(localRecovery),
    "state.checkpoints.num-retained" -> later(checkpointNumRetained.ensureIntMin(1)),
  )
}

object StateBackendType extends ComplexEnum {
  type StateBackendType = Value
  val Hashmap = Value("hashmap")
  val Rocksdb = Value("rocksdb")
}

object CheckpointStorageType extends ComplexEnum {
  type CheckpointStorageType = Value
  val Jobmanager = Value("jobmanager")
  val Filesystem = Value("filesystem")
}

/**
 * Flink Jobmanager HA configuration.
 */
case class JmHaConf(
    haImplClz: String = "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
    storageDir: String,
    clusterId: Option[String] = None)
    extends FlinkRawConf {

  def rawMapping = Vector(
    "high-availability"            -> later(haImplClz),
    "high-availability.storageDir" -> later(storageDir),
    "high-availability.cluster-id" -> later(clusterId)
  )
}

/**
 * s3 storage access configuration.
 */
case class S3AccessConf(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    pathStyleAccess: Option[Boolean] = None,
    sslEnabled: Option[Boolean] = None)
    extends FlinkRawConf {

  def rawMapping = Vector(
    "s3.endpoint"          -> later(endpoint),
    "s3.access-key"        -> later(accessKey),
    "s3.secret-key"        -> later(secretKey),
    "s3.path.style.access" -> later(pathStyleAccess),
    "s3.ssl.enabled"       -> later(sslEnabled)
  )
}

object S3AccessConf {
  def apply(conf: S3Conf): S3AccessConf =
    S3AccessConf(conf.endpoint, conf.accessKey, conf.secretKey, Some(conf.pathStyleAccess), Some(conf.sslEnabled))
}
