package kce.flink.operator
import com.coralogix.zio.k8s.client.K8sFailure.syntax.K8sZIOSyntax
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import kce.common.LogMessageTool.LogMessageStringWrapper
import kce.common.ziox.usingAttempt
import kce.conf.KceConf
import kce.flink.operator.FlinkConfigExtension.configurationToPF
import kce.flink.operator.FlinkOprHelper.getClusterClientFactory
import kce.flink.operator.PodTemplateResolver.resolvePodTemplateAndDump
import kce.flink.operator.entity.FlinkExecMode.K8sSession
import kce.flink.operator.entity.{FlinkAppClusterDef, FlinkRestSvcEndpoint, FlinkSessClusterDef}
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import zio.ZIO.{attempt, attemptBlocking, attemptBlockingInterrupt, logInfo, scoped, succeed}
import zio.{IO, ZIO}

/**
 * Default FlinkK8sOperator implementation.
 */
class FlinkK8sOperatorLive(kceConf: KceConf, k8sClient: Kubernetes) extends FlinkK8sOperator {

  /**
   * Deploy Flink Application cluster.
   */
  override def deployApplicationCluster(definition: FlinkAppClusterDef): IO[FlinkOprErr, Unit] = {
    for {
      clusterDef      <- succeed(definition.revise())
      podTemplateFile <- resolvePodTemplateAndDump(clusterDef, kceConf)
      // convert to effective flink configuration
      rawConfig <- succeed(
        clusterDef
          .toFlinkRawConfig(kceConf)
          .append("kubernetes.pod-template-file.jobmanager", podTemplateFile)
          .append("kubernetes.pod-template-file.taskmanager", podTemplateFile)
          .append("$internal.deployment.config-dir", kceConf.flink.logConfDir)
      )
      _ <- logInfo(s"Deploy Flink Session Cluster:\n${rawConfig.toPrettyPrint}".stripMargin)
      // deploy app cluster
      _ <- ZIO
        .scoped {
          for {
            clusterClientFactory <- getClusterClientFactory(K8sSession)
            clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
            appConfiguration     <- attempt(new ApplicationConfiguration(clusterDef.appArgs.toArray, clusterDef.appMain.orNull))
            k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
            _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
          } yield ()
        }
        .mapError(SubmitFlinkClusterErr("Fail to submit flink application cluster to kubernetes." <> definition.logTags, _))
      _ <- logInfo(s"Deploy Flink session cluster successfully." <> definition.logTags)
    } yield ()
  }

  /**
   * Deploy Flink session cluster.
   */
  override def deploySessionCluster(definition: FlinkSessClusterDef): IO[FlinkOprErr, Unit] = {
    for {
      clusterDef      <- succeed(definition.revise())
      podTemplateFile <- resolvePodTemplateAndDump(clusterDef, kceConf)
      // convert to effective flink configuration
      rawConfig <- succeed(
        clusterDef
          .toFlinkRawConfig(kceConf)
          .append("kubernetes.pod-template-file.jobmanager", podTemplateFile)
          .append("kubernetes.pod-template-file.taskmanager", podTemplateFile)
          .append("$internal.deployment.config-dir", kceConf.flink.logConfDir)
      )
      _ <- logInfo(s"Deploy Flink Application Cluster:\n${rawConfig.toPrettyPrint}\n".stripMargin)
      // deploy cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getClusterClientFactory(K8sSession)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlocking(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
        } yield ()
      }.mapError(SubmitFlinkClusterErr("Fail to submit flink session cluster to kubernetes." <> definition.logTags, _))
    } yield ()
  }

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  override def retrieveRestEndpoint(clusterId: String, namespace: String): IO[FlinkOprErr, Option[FlinkRestSvcEndpoint]] = {
    k8sClient.v1.services
      .get(s"$clusterId-rest", namespace)
      .ifFound
      .flatMap {
        case None => ZIO.succeed(None)
        case Some(svc) =>
          for {
            metadata  <- svc.getMetadata
            name      <- metadata.getName
            ns        <- metadata.getNamespace
            spec      <- svc.getSpec
            clusterIp <- spec.getClusterIP
            ports     <- spec.getPorts
            restPort = ports
              .find(_.port == 8081)
              .flatMap(_.targetPort.map(_.value.fold(identity, _.toInt)).toOption)
              .getOrElse(8081)
          } yield Some(FlinkRestSvcEndpoint(name, ns, restPort, clusterIp))
      }
      .mapError(RequestK8sApiErr("Retrieve flink rest svc failed." <> ("clusterId" -> clusterId, "namespace" -> namespace), _))
  }

}
