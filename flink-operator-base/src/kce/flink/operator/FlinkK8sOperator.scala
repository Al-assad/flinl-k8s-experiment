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
  def deployApplicationCluster(definition: FlinkAppClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessionCluster(definition: FlinkSessClusterDef): IO[FlinkOprErr, Unit]

  // TODO
  // def submitJobToSession()

  // def killCluster

  // def cancelJob

  // def getJobStatus

  // def listJobs

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieveRestEndpoint(clusterId: String, namespace: String): IO[FlinkOprErr, Option[FlinkRestSvcEndpoint]]
}

object FlinkK8sOperator {
  val live = ZLayer {
    for {
      kceConf   <- ZIO.service[KceConf]
      k8sClient <- ZIO.service[Kubernetes]
    } yield new FlinkK8sOperatorLive(kceConf, k8sClient)
  }
}
