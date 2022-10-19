package kce.flink.operator
import com.coralogix.zio.k8s.client.K8sFailure
import com.coralogix.zio.k8s.client.v1.services.Services
import kce.conf.KceConf
import kce.flink.operator.FlinkConfigExtension.configurationToPF
import kce.flink.operator.entity.{FlinkRestSvcEndpoint, FlinkSessDef}
import zio.{IO, ZIO}

object FlinkK8sOperatorImpl extends FlinkK8sOperator {

  override def deployApplication(): IO[Throwable, Unit] = ???

  /**
   * Deploy Flink session cluster.
   */
  override def deploySessionCluster(definition: FlinkSessDef): IO[Throwable, Unit] = {
    // generate pod template

    // convert to flink configuration
    val rawConfig = definition.toFlinkRawConfig
      .append("$internal.deployment.config-dir", KceConf.default.flinkLogConfDir)
    ???
  }

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  override def retrieveRestEndpoint(clusterId: String, namespace: String): ZIO[Services, K8sFailure, FlinkRestSvcEndpoint] =
    for {
      svcs      <- ZIO.service[Services.Service]
      svc       <- svcs.get(s"$clusterId-rest", namespace)
      metadata  <- svc.getMetadata
      name      <- metadata.getName
      ns        <- metadata.getNamespace
      spec      <- svc.getSpec
      clusterIp <- spec.getClusterIP
      ports     <- spec.getPorts
      restPort = ports
        .find(_.port == 8081)
        .flatMap(_.targetPort.toOption)
        .map(_.value.fold(identity, _.toInt))
        .getOrElse(8081)
    } yield FlinkRestSvcEndpoint(name, ns, restPort, clusterIp)
}
