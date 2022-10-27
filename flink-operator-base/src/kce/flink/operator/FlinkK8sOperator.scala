package kce.flink.operator

import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import kce.conf.PotaConf
import kce.flink.operator.entity.{FlinkAppClusterDef, FlinkRestSvcEndpoint, FlinkSessClusterDef, FlinkSessJobDef}
import kce.fs.S3Operator
import zio._
import zio.macros.accessible

/**
 * Flink on Kubernetes Operator.
 */
@accessible
trait FlinkK8sOperator {

  /**
   * Deploy Flink Application cluster.
   * TODO support restore config.
   */
  def deployApplicationCluster(definition: FlinkAppClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessionCluster(definition: FlinkSessClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Submit job to Flink session cluster.
   * @return flink job-id
   */
  def submitJobToSession(definition: FlinkSessJobDef): IO[FlinkOprErr, String]

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
      kceConf    <- ZIO.service[PotaConf]
      k8sClient  <- ZIO.service[Kubernetes]
      s3Operator <- ZIO.service[S3Operator]
    } yield new FlinkK8sOperatorLive(kceConf, k8sClient, s3Operator)
  }
}
