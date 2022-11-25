package potamoi.flink.operator

import potamoi.common.PathTool.{getFileName, isS3Path}
import potamoi.common.ZIOExtension.usingAttempt
import potamoi.config.PotaConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkConfigExtension.configurationToPF
import potamoi.flink.operator.FlinkRestRequest.{RunJobReq, StopJobSptReq, TriggerSptReq}
import potamoi.flink.share.FlinkOprErr.{NotSupportJobJarPath, SubmitFlinkApplicationClusterErr, UnableToResolveS3Resource}
import potamoi.flink.share.model.FlinkExecMode.K8sSession
import potamoi.flink.share.model._
import potamoi.flink.share.{FlinkIO, JobId, TriggerId}
import potamoi.fs.{lfs, S3Operator}
import potamoi.k8s.K8sClient
import potamoi.syntax._
import zio.ZIO
import zio.ZIO.{attempt, attemptBlockingInterrupt, logInfo, scoped}
import zio.ZIOAspect.annotated

/**
 * Flink session mode cluster operator.
 */
trait FlinkSessClusterOperator {

  /**
   * Deploy Flink session cluster.
   */
  def deployCluster(definition: FlinkSessClusterDef): FlinkIO[Unit]

  /**
   * Submit job to Flink session cluster.
   */
  def submitJob(definition: FlinkSessJobDef): FlinkIO[JobId]

  /**
   * Cancel job in flink session cluster.
   */
  def cancelJob(fjid: Fjid): FlinkIO[Unit]

  /**
   * Stop job in flink session cluster with savepoint.
   */
  def stopJob(fjid: Fjid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fjid: Fjid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)]

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): FlinkIO[Unit]
}

case class FlinkSessClusterOperatorImpl(potaConf: PotaConf, k8sClient: K8sClient, s3Operator: S3Operator, flinkObserver: FlinkObserver)
    extends FlinkBaseOperator(potaConf, k8sClient, flinkObserver) with FlinkSessClusterOperator {

  /**
   * Deploy Flink session cluster.
   */
  override def deployCluster(clusterDef: FlinkSessClusterDef): FlinkIO[Unit] = {
    for {
      clusterDef <- clDefResolver.session.revise(clusterDef)
      // resolve flink pod template and log config
      podTemplateFilePath <- podTemplateFileOutputPath(clusterDef)
      logConfFilePath     <- logConfFileOutputPath(clusterDef)
      _                   <- podTplResolver.resolvePodTemplateAndDump(clusterDef, potaConf, podTemplateFilePath)
      _                   <- logConfResolver.ensureFlinkLogsConfigFiles(logConfFilePath, overwrite = true)
      // convert to effective flink configuration
      rawConfig <- clDefResolver.session.toFlinkRawConfig(clusterDef, potaConf).map { conf =>
        conf
          .append("kubernetes.pod-template-file.jobmanager", podTemplateFilePath)
          .append("kubernetes.pod-template-file.taskmanager", podTemplateFilePath)
          .append("$internal.deployment.config-dir", logConfFilePath)
      }
      _ <- logInfo(s"Start to deploy flink application cluster:\n${rawConfig.toMap(true).toPrettyStr}".stripMargin)
      // deploy cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getFlinkClusterClientFactory(K8sSession)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
        } yield ()
      }.mapError(SubmitFlinkApplicationClusterErr(clusterDef.fcid, _))
      // tracking cluster
      _ <- flinkObserver.manager.trackCluster(clusterDef.fcid).ignore
      _ <- logInfo(s"Deploy flink application cluster successfully.")
    } yield ()
  } @@ annotated(clusterDef.fcid.toAnno: _*)

  /**
   * Submit job to Flink session cluster.
   */
  override def submitJob(jobDef: FlinkSessJobDef): FlinkIO[JobId] = {
    for {
      // get rest api url of session cluster
      restUrl <- flinkObserver.restEndpoint.get(jobDef.clusterId -> jobDef.namespace, directly = true).map(_.chooseUrl)
      _       <- logInfo(s"Connect flink rest service: $restUrl")
      _       <- ZIO.fail(NotSupportJobJarPath(jobDef.jobJar)).unless(isS3Path(jobDef.jobJar))

      // download job jar
      _ <- logInfo(s"Downloading flink job jar from s3 storage: ${jobDef.jobJar}")
      jobJarPath <- s3Operator
        .download(jobDef.jobJar, s"${potaConf.flink.localTmpDir}/${jobDef.namespace}@${jobDef.clusterId}/${getFileName(jobDef.jobJar)}")
        .mapBoth(UnableToResolveS3Resource, _.getPath)

      // submit job
      _ <- logInfo(s"Start to submit job to flink cluster: \n${jobDef.toPrettyStr}".stripMargin)
      jobId <- for {
        _ <- logInfo(s"Uploading flink job jar to flink cluster, path: $jobJarPath, flink-rest: $restUrl")
        rest = flinkRest(restUrl)
        jarId <- rest.uploadJar(jobJarPath)
        jobId <- rest.runJar(jarId, RunJobReq(jobDef))
        _     <- rest.deleteJar(jarId).ignore
      } yield jobId

      _ <- lfs.rm(jobJarPath).ignore
      _ <- logInfo(s"Submit job to flink session cluster successfully, jobId: $jobId")
    } yield jobId
  } @@ annotated(Fcid(jobDef.clusterId, jobDef.namespace).toAnno: _*)

  /**
   * Cancel job in flink session cluster.
   */
  override def cancelJob(fjid: Fjid): FlinkIO[Unit] = {
    for {
      restUrl <- flinkObserver.restEndpoint.get(fjid.fcid, directly = true).map(_.chooseUrl)
      _       <- flinkRest(restUrl).cancelJob(fjid.jobId)
    } yield ()
  } @@ annotated(fjid.toAnno: _*)

  /**
   * Stop job in flink session cluster with savepoint.
   */
  override def stopJob(fjid: Fjid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.restEndpoint.get(fjid.fcid, directly = true).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(fjid.jobId, StopJobSptReq(savepoint))
    } yield fjid -> triggerId
  } @@ annotated(fjid.toAnno: _*)

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(fjid: Fjid, savepoint: FlinkJobSptDef): FlinkIO[(Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.restEndpoint.get(fjid.fcid, directly = true).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).triggerSavepoint(fjid.jobId, TriggerSptReq(savepoint))
    } yield fjid -> triggerId
  } @@ annotated(fjid.toAnno: _*)

}
