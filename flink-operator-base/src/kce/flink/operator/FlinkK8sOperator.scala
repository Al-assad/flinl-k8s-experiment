package kce.flink.operator

import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import kce.conf.KceConf
import kce.flink.operator.entity.{FlinkAppClusterDef, FlinkRestSvcEndpoint, FlinkSessClusterDef}
import zio._
import zio.macros.accessible

/**
 * Flink on Kubernetes Operator.
 */
@accessible
trait FlinkK8sOperator {

  /**
   * Deploy Flink Application cluster.
   */
  def deployApplication(definition: FlinkAppClusterDef): IO[Throwable, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessionCluster(definition: FlinkSessClusterDef): IO[Throwable, Unit]

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieveRestEndpoint(clusterId: String, namespace: String): ZIO[Any, Throwable, FlinkRestSvcEndpoint]
}

object FlinkK8sOperator {
  val live = ZLayer {
    for {
      kceConf   <- ZIO.service[KceConf]
      k8sClient <- ZIO.service[Kubernetes]
    } yield new FlinkK8sOperatorLive(kceConf, k8sClient)
  }
}
