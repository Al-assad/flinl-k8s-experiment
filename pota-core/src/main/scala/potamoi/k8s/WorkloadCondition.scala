package potamoi.k8s

import potamoi.common.ComplexEnum
import potamoi.k8s.WorkloadCondStatus.WorkloadCondStatus
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes workload conditions.
 * see: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions
 */
case class WorkloadCondition(
    condType: String,
    status: WorkloadCondStatus,
    reason: Option[String],
    message: Option[String],
    lastTransitionTime: Option[Long])

object WorkloadCondition {
  implicit val codec: JsonCodec[WorkloadCondition]   = DeriveJsonCodec.gen[WorkloadCondition]
  implicit val ordering: Ordering[WorkloadCondition] = Ordering.by(e => e.lastTransitionTime.getOrElse(0L))
}

object WorkloadCondStatus extends ComplexEnum {
  type WorkloadCondStatus = Value
  val True, False, Unknown = Value
}
