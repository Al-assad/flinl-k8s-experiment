package kce.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import kce.common.Err

/**
 * Flink operation error.
 */
sealed abstract class FlinkOprErr(msg: String, cause: Throwable) extends Exception(msg, cause)

case class PodTemplateResolveErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)

case class SubmitFlinkClusterErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)

case class RequestK8sApiErr(msg: String, k8sFailure: K8sFailure) extends FlinkOprErr(msg, Err(k8sFailure.toString))
