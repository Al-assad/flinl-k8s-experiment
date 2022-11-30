package potamoi.flink.operator

import org.apache.flink.client.deployment.application.ApplicationConfiguration
import potamoi.common.ZIOExtension.usingAttempt
import potamoi.config.PotaConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkConfigExtension.{configurationToPF, InjectedDeploySourceConf, InjectedExecModeKey}
import potamoi.flink.operator.FlinkRestRequest.{StopJobSptReq, TriggerSptReq}
import potamoi.flink.share.FlinkOprErr.{EmptyJobInCluster, SubmitFlinkApplicationClusterErr}
import potamoi.flink.share.model.FlinkExecMode.K8sApp
import potamoi.flink.share.model.{Fcid, Fjid, FlinkAppClusterDef, FlinkExecMode, FlinkJobSptDef}
import potamoi.flink.share.{FlinkIO, TriggerId}
import potamoi.fs.S3Operator
import potamoi.k8s.K8sClient
import potamoi.syntax._
import zio.ZIO.{attempt, attemptBlockingInterrupt, fail, logInfo, scoped, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink application mode cluster operator.
 */
trait FlinkAppClusterOperator {

  /**
   * Deploy Flink application cluster.
   */
  def deployCluster(definition: FlinkAppClusterDef): FlinkIO[Unit]

  /**
   * Cancel job in flink application cluster.
   */
  def cancelJob(fcid: Fcid): FlinkIO[Unit]

  /**
   * Stop job in flink application cluster with savepoint.
   */
  def stopJob(fcid: Fcid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fcid: Fcid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)]

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): FlinkIO[Unit]

}

case class FlinkAppClusterOperatorImpl(potaConf: PotaConf, k8sClient: K8sClient, s3Operator: S3Operator, flinkObserver: FlinkObserver)
    extends FlinkBaseOperator(potaConf, k8sClient, flinkObserver) with FlinkAppClusterOperator {

  /**
   * Deploy Flink application cluster.
   */
  override def deployCluster(clusterDef: FlinkAppClusterDef): FlinkIO[Unit] = {
    for {
      clusterDef <- clDefResolver.application.revise(clusterDef)
      // resolve flink pod template and log config
      podTemplateFilePath <- podTemplateFileOutputPath(clusterDef)
      logConfFilePath     <- logConfFileOutputPath(clusterDef)
      _                   <- podTplResolver.resolvePodTemplateAndDump(clusterDef, potaConf, podTemplateFilePath)
      _                   <- logConfResolver.ensureFlinkLogsConfigFiles(logConfFilePath, overwrite = true)
      // convert to effective flink configuration
      rawConfig <- clDefResolver.application.toFlinkRawConfig(clusterDef, potaConf).map { conf =>
        conf
          .append("kubernetes.pod-template-file.jobmanager", podTemplateFilePath)
          .append("kubernetes.pod-template-file.taskmanager", podTemplateFilePath)
          .append("$internal.deployment.config-dir", logConfFilePath)
          .append(InjectedExecModeKey, FlinkExecMode.K8sApp.toString)
          .append(InjectedDeploySourceConf._1, InjectedDeploySourceConf._2)
      }
      _ <- logInfo(s"Start to deploy flink session cluster:\n${rawConfig.toMap(true).toPrettyStr}".stripMargin)
      // deploy app cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getFlinkClusterClientFactory(K8sApp)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          appConfiguration     <- attempt(new ApplicationConfiguration(clusterDef.appArgs.toArray, clusterDef.appMain.orNull))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
        } yield ()
      }.mapError(SubmitFlinkApplicationClusterErr(clusterDef.fcid, _))
      // tracking cluster
      _ <- flinkObserver.manager.trackCluster(clusterDef.fcid).ignore
      _ <- logInfo(s"Deploy flink session cluster successfully.")
    } yield ()
  } @@ annotated(clusterDef.fcid.toAnno: _*)

  /**
   * Cancel job in flink application cluster.
   */
  override def cancelJob(fcid: Fcid): FlinkIO[Unit] = {
    for {
      restUrl <- flinkObserver.restEndpoints.get(fcid, directly = true).map(_.chooseUrl)
      jobId   <- findJobIdFromAppCluster(fcid)
      _       <- flinkRest(restUrl).cancelJob(jobId)
    } yield ()
  } @@ annotated(fcid.toAnno: _*)

  /**
   * Stop job in flink application cluster with savepoint.
   */
  override def stopJob(fcid: Fcid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.restEndpoints.get(fcid, directly = true).map(_.chooseUrl)
      jobId     <- findJobIdFromAppCluster(fcid)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(jobId, StopJobSptReq(savepoint))
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ annotated(fcid.toAnno: _*)

  private def findJobIdFromAppCluster(fcid: Fcid): FlinkIO[String] =
    flinkObserver.jobs
      .listJobId(fcid)
      .map(_.headOption)
      .flatMap {
        case None        => fail(EmptyJobInCluster(fcid))
        case Some(jobId) => succeed(jobId)
      }
      .tap(jobId => logInfo(s"Found job-id in flink application cluster: $jobId "))

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(fcid: Fcid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.restEndpoints.get(fcid, directly = true).map(_.chooseUrl)
      jobId     <- findJobIdFromAppCluster(fcid)
      triggerId <- flinkRest(restUrl).triggerSavepoint(jobId, TriggerSptReq(savepoint))
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ annotated(fcid.toAnno: _*)

}
