package kce.flink.operator.entity

import kce.common.ComplexEnum
import kce.flink.operator.entity.SavepointRestoreMode.{Claim, SavepointRestoreMode}

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
    savepointRestore: Option[SavepointRestoreConf] = None) {

  def logTags = Map("clusterId" -> clusterId, "namespace" -> namespace, "jobJar" -> jobJar)
}

case class SavepointRestoreConf(
    savepointPath: String,
    allowNonRestoredState: Boolean = false,
    restoreMode: SavepointRestoreMode = Claim
)

/**
 * See [[org.apache.flink.runtime.jobgraph.RestoreMode]]
 */
object SavepointRestoreMode extends ComplexEnum {
  type SavepointRestoreMode = Value
  val Claim   = Value("CLAIM")
  val NoClaim = Value("NO_CLAIM")
  val Legacy  = Value("LEGACY")
}
