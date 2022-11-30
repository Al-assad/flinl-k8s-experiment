package potamoi.k8s

import potamoi.common.ComplexEnum

/**
 * Kubernetes pod phase.
 * see: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
 */
object PodPhase extends ComplexEnum {
  type PodPhase = Value
  val Pending, Running, Succeeded, Failed, Unknown = Value
}