package kce.flink.operator
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import kce.common.ziox.usingAttempt
import kce.conf.KceConf
import FlinkConfigExtension.configurationToPF
import kce.flink.operator.FlinkOprHelper.getClusterClientFactory
import kce.flink.operator.PodTemplateResolver.resolvePodTemplateAndDump
import kce.flink.operator.entity.FlinkExecMode.K8sSession
import kce.flink.operator.entity.{FlinkAppClusterDef, FlinkRestSvcEndpoint, FlinkSessClusterDef}
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import zio.ZIO.{attempt, attemptBlockingInterrupt, scoped}
import zio.{IO, ZIO}

class FlinkK8sOperatorLive(kceConf: KceConf, k8sClient: Kubernetes) extends FlinkK8sOperator {

  /**
   * Deploy Flink Application cluster.
   */
  override def deployApplication(definition: FlinkAppClusterDef): IO[Throwable, Unit] =
    for {
      clusterDef      <- attempt(definition.revise())
      podTemplateFile <- resolvePodTemplateAndDump(clusterDef, kceConf)
      // convert to effective flink configuration
      rawConfig <- attempt(
        clusterDef
          .toFlinkRawConfig(kceConf)
          .append("kubernetes.pod-template-file", podTemplateFile)
          .append("$internal.deployment.config-dir", kceConf.flink.logConfDir))
      // deploy app cluster
      _ <- ZIO.scoped {
        for {
          clusterClientFactory <- getClusterClientFactory(K8sSession)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          appConfiguration     <- attempt(new ApplicationConfiguration(clusterDef.appArgs.toArray, clusterDef.appMain.orNull))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
        } yield ()
      }
    } yield ()

  /**
   * Deploy Flink session cluster.
   */
  override def deploySessionCluster(definition: FlinkSessClusterDef): IO[Throwable, Unit] =
    for {
      clusterDef      <- attempt(definition.revise())
      podTemplateFile <- resolvePodTemplateAndDump(clusterDef, kceConf)
      // convert to effective flink configuration
      rawConfig <- attempt(
        clusterDef
          .toFlinkRawConfig(kceConf)
          .append("kubernetes.pod-template-file", podTemplateFile)
          .append("$internal.deployment.config-dir", kceConf.flink.logConfDir)
      )
      // deploy cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getClusterClientFactory(K8sSession)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
        } yield ()
      }
    } yield ()

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  override def retrieveRestEndpoint(clusterId: String, namespace: String): IO[Throwable, FlinkRestSvcEndpoint] = {
    val ef = for {
      svc       <- k8sClient.v1.services.get(s"$clusterId-rest", namespace)
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
    ef.mapError(f => new RuntimeException(f.toString))
  }
}
