package kce.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import com.coralogix.zio.k8s.client.v1.services.Services
import kce.flink.operator.entity.{FlinkRestSvcEndpoint, FlinkSessDef}
import zio._

trait FlinkK8sOperator {

  def deployApplication(): IO[Throwable, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessionCluster(definition: FlinkSessDef): IO[Throwable, Unit]

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieveRestEndpoint(clusterId: String, namespace: String): ZIO[Services, K8sFailure, FlinkRestSvcEndpoint]
}
