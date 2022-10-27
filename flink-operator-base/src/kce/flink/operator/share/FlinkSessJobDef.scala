package kce.flink.operator.share

import kce.common.ComplexEnum
import kce.flink.operator.share.SavepointRestoreMode.{Claim, SavepointRestoreMode}

/**
 * Definition of the job submitted to Flink session cluster.
 */
case class FlinkSessJobDef(
    clusterId: String,
    namespace: String,
    jobJar: String,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    parallelism: Option[Int] = None,
    savepointRestore: Option[SavepointRestoreConf] = None)

/**
 * @see [[org.apache.flink.runtime.jobgraph.SavepointRestoreSettings]]
 */
case class SavepointRestoreConf(
    savepointPath: String,
    allowNonRestoredState: Boolean = false,
    restoreMode: SavepointRestoreMode = Claim
)

/**
 * @see [[org.apache.flink.runtime.jobgraph.RestoreMode]]
 */
object SavepointRestoreMode extends ComplexEnum {
  type SavepointRestoreMode = Value
  val Claim   = Value("CLAIM")
  val NoClaim = Value("NO_CLAIM")
  val Legacy  = Value("LEGACY")
}
