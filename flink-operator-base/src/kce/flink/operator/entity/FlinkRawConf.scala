package kce.flink.operator.entity

import kce.common.ComplexEnum
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
  def toRaw: Configuration       = injectRaw(EmptyConfiguration())
  def toMap: Map[String, String] = toRaw.toMap.asScala.toMap
  def injectRaw: ConfigurationPF => ConfigurationPF
}

object FlinkRowConf {
  implicit val cpuConfCodec: JsonCodec[CpuConf]               = DeriveJsonCodec.gen[CpuConf]
  implicit val memConfCodec: JsonCodec[MemConf]               = DeriveJsonCodec.gen[MemConf]
  implicit val parConfCodec: JsonCodec[ParConf]               = DeriveJsonCodec.gen[ParConf]
  implicit val webUICodec: JsonCodec[WebUIConf]               = DeriveJsonCodec.gen[WebUIConf]
  implicit val restartStgCodec: JsonCodec[RestartStgConf]     = DeriveJsonCodec.gen[RestartStgConf]
  implicit val stateBackendCodec: JsonCodec[StateBackendConf] = DeriveJsonCodec.gen[StateBackendConf]
  implicit val jmHaCodec: JsonCodec[JmHaConf]                 = DeriveJsonCodec.gen[JmHaConf]
}

/**
 * Flink k8s cpu configuration.
 */
case class CpuConf(jm: Double = 1.0, tm: Double = -1.0, jmFactor: Double = 1.0, tmFactor: Double = 1.0) extends FlinkRawConf {
  def injectRaw = conf =>
    conf
      .append("kubernetes.jobmanager.cpu", jm)
      .append("kubernetes.jobmanager.cpu.limit-factor", jmFactor)
      .append("kubernetes.taskmanager.cpu", tm)
      .append("kubernetes.taskmanager.cpu.limit-factor", tmFactor)

}

/**
 * Flink parallelism configuration.
 */
case class ParConf(numOfSlot: Int = 1, parDefault: Int = 1) extends FlinkRawConf {
  def injectRaw = conf =>
    conf
      .append("taskmanager.numberOfTaskSlots", numOfSlot)
      .append("parallelism.default", parDefault)
}

/**
 * Flink memory configuration.
 */
case class MemConf(jm: Int = 1600, tm: Int = 1728) extends FlinkRawConf {
  def injectRaw = conf =>
    conf
      .append("jobmanager.memory.process.size", s"$jm m")
      .append("taskmanager.memory.process.size", s"$tm m")

}

/**
 * Flink web ui service configuration.
 */
case class WebUIConf(enableSubmit: Boolean = true, enableCancel: Boolean = true) extends FlinkRawConf {
  def injectRaw = row =>
    row
      .append("web.submit.enable", enableSubmit)
      .append("web.cancel.enable", enableCancel)
}

/**
 * Flink task restart strategy.
 */
sealed trait RestartStgConf extends FlinkRawConf

case object NonRestartStg extends RestartStgConf {
  def injectRaw =
    _.append("restart-strategy", "none")
}

case class FixedDelayRestartStg(attempts: Int = 1, delaySec: Int = 1) extends RestartStgConf {
  def injectRaw = conf =>
    conf
      .append("restart-strategy", "fixed-delay")
      .append("restart-strategy.fixed-delay.attempts", attempts)
      .append("restart-strategy.fixed-delay.delay", s"$delaySec s")
}

case class FailureRateRestartStg(delaySec: Int = 1, failureRateIntervalSec: Int = 60, maxFailuresPerInterval: Int = 1) extends RestartStgConf {
  def injectRaw = conf =>
    conf
      .append("restart-strategy", "failure-rate")
      .append("restart-strategy.failure-rate.delay", s"$delaySec s")
      .append("restart-strategy.failure-rate.failure-rate-interval", s"$failureRateIntervalSec s")
      .append("restart-strategy.failure-rate.max-failures-per-interval", maxFailuresPerInterval)
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

  def injectRaw = conf =>
    conf
      .append("state.backend", backendType.toString)
      .append("state.checkpoint-storage", checkpointStorage.toString)
      .append("state.checkpoints.dir", checkpointDir)
      .append("state.savepoints.dir", savepointDir)
      .append("state.backend.incremental", incremental)
      .append("state.backend.local-recovery", localRecovery)
      .append("state.checkpoints.num-retained", checkpointNumRetained)
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
case class JmHaConf(storageDir: String, clusterId: Option[String] = None) extends FlinkRawConf {
  def injectRaw = conf =>
    conf
      .append("high-availability", "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory")
      .append("high-availability.storageDir", storageDir)
      .appendWhen(clusterId.isDefined)("high-availability.cluster-id", clusterId.get)
}
