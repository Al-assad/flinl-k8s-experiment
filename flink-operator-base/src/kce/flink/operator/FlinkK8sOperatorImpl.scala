package kce.flink.operator
import com.coralogix.zio.k8s.client.K8sFailure
import com.coralogix.zio.k8s.client.v1.services.Services
import kce.common.ZIOExtension.usingAttempt
import kce.conf.KceConf
import kce.flink.operator.FlinkConfigExtension.configurationToPF
import kce.flink.operator.entity.FlinkExecMode.K8sSession
import kce.flink.operator.FlinkOprHelper.getClusterClientFactory
import kce.flink.operator.entity.{FlinkRestSvcEndpoint, FlinkSessClusterDef}
import zio.ZIO
import zio.ZIO.attempt

object FlinkK8sOperatorImpl extends FlinkK8sOperator {

  override def deployApplication(): ZIO[KceConf, Throwable, Unit] = ???

  // todo replace jobJar form s3 to local
  // file:// => not handle
  // s3:// => add to lib -> set to file://

  /**
   * Deploy Flink session cluster.
   */
  override def deploySessionCluster(definition: FlinkSessClusterDef): ZIO[KceConf, Throwable, Unit] =
    for {
      conf <- ZIO.service[KceConf]
//      clusterDef = definition.revise()
      clusterDef = definition

      // generate pod-template and store it in local tmp dir
      podTemplate <- PodTemplateResolver.resolvePodTemplate(clusterDef)
      podTemplatePath = s"${conf.flink.localTmpDir}/${clusterDef.namespace}@${clusterDef.namespace}/flink-podtemplate.yaml"
      _ <- PodTemplateResolver.writeToLocal(podTemplate, podTemplatePath)

      // convert to flink configuration
      rawConfig = clusterDef
        .toFlinkRawConfig(conf)
        .append("kubernetes.pod-template-file", podTemplatePath)
        .append("$internal.deployment.config-dir", conf.flink.logConfDir)

      // deploy cluster
      _ <- ZIO.scoped {
        for {
          clusterClientFactory <- getClusterClientFactory(K8sSession)
          clusterSpecification = clusterClientFactory.getClusterSpecification(rawConfig)
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attempt(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
        } yield ()
      }
    } yield ()

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
