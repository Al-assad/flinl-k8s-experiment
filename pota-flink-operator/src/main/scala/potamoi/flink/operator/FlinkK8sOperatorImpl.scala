package potamoi.flink.operator

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import potamoi.common.PathTool.{getFileName, isS3Path}
import potamoi.common.ZIOExtension.usingAttempt
import potamoi.conf.PotaConf
import potamoi.flink.observer.FlinkK8sObserver
import potamoi.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import potamoi.flink.operator.FlinkOprErr._
import potamoi.flink.operator.FlinkRestRequest.{RunJobReq, StopJobSptReq, TriggerSptReq}
import potamoi.flink.share.FlinkExecMode.{FlinkExecMode, K8sSession}
import potamoi.flink.share._
import potamoi.fs.{lfs, S3Operator}
import potamoi.k8s.stringToK8sNamespace
import potamoi.syntax._
import zio.ZIO.{attempt, attemptBlockingInterrupt, fail, logInfo, scoped, succeed}
import zio._

import scala.language.implicitConversions

/**
 * Default FlinkK8sOperator implementation.
 */
object FlinkK8sOperatorImpl {
  val live = ZLayer {
    for {
      potaConf <- ZIO.service[PotaConf]
      k8sClient <- ZIO.service[Kubernetes]
      s3Operator <- ZIO.service[S3Operator]
      flinkObserver <- ZIO.service[FlinkK8sObserver]
    } yield new FlinkK8sOperatorImpl(potaConf, k8sClient, s3Operator, flinkObserver)
  }
}

class FlinkK8sOperatorImpl(potaConf: PotaConf, k8sClient: Kubernetes, s3Operator: S3Operator, flinkObserver: FlinkK8sObserver)
    extends FlinkK8sOperator {

  implicit val flinkConf               = potaConf.flink
  private val flinkClusterClientLoader = new DefaultClusterClientServiceLoader()

  /**
   * Local workplace directory for each Flink cluster.
   */
  private def clusterLocalWp(clusterId: String, namespace: String): UIO[String] =
    succeed(s"${potaConf.flink.localTmpDir}/${namespace}@${clusterId}")

  /**
   * Local Generated flink kubernetes pod-template file output path.
   */
  private def podTemplateFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWp(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/flink-podtemplate.yaml")

  /**
   * Local Generated flink kubernetes config file output path.
   */
  private def logConfFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWp(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/log-conf")

  /**
   * Get Flink ClusterClientFactory by execution mode.
   */
  private def getFlinkClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] = ZIO.attempt {
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    flinkClusterClientLoader.getClusterClientFactory(conf)
  }

  /**
   * Deploy Flink Application cluster.
   */
  override def deployAppCluster(clusterDef: FlinkAppClusterDef): IO[FlinkOprErr, Unit] = {
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
      }
      _ <- logInfo(s"Start to deploy flink session cluster:\n${rawConfig.toMap(true).toPrettyString}".stripMargin)
      // deploy app cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getFlinkClusterClientFactory(K8sSession)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          appConfiguration     <- attempt(new ApplicationConfiguration(clusterDef.appArgs.toArray, clusterDef.appMain.orNull))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
        } yield ()
      }.mapError(SubmitFlinkSessionClusterErr(clusterDef.fcid, _))
      // tracking cluster
      _ <- flinkObserver.trackCluster(clusterDef.fcid).ignore
      _ <- logInfo(s"Deploy flink session cluster successfully.")
    } yield ()
  } @@ ZIOAspect.annotated(clusterDef.fcid.toAnno: _*)

  /**
   * Deploy Flink session cluster.
   */
  override def deploySessCluster(clusterDef: FlinkSessClusterDef): IO[FlinkOprErr, Unit] = {
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
      _ <- logInfo(s"Start to deploy flink application cluster:\n${rawConfig.toMap(true).toPrettyString}".stripMargin)
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
      _ <- flinkObserver.trackCluster(clusterDef.fcid).ignore
      _ <- logInfo(s"Deploy flink application cluster successfully.")
    } yield ()
  } @@ ZIOAspect.annotated(clusterDef.fcid.toAnno: _*)

  /**
   * Submit job to Flink session cluster.
   */
  override def submitJobToSession(jobDef: FlinkSessJobDef): IO[FlinkOprErr, JobId] = {
    for {
      // get rest api url of session cluster
      restUrl <- flinkObserver.retrieveRestEndpoint(jobDef.clusterId -> jobDef.namespace, directly = true).map(_.chooseUrl)
      _       <- logInfo(s"Connect flink rest service: $restUrl")
      _       <- ZIO.fail(NotSupportJobJarPath(jobDef.jobJar)).unless(isS3Path(jobDef.jobJar))

      // download job jar
      _ <- logInfo(s"Downloading flink job jar from s3 storage: ${jobDef.jobJar}")
      jobJarPath <- s3Operator
        .download(jobDef.jobJar, s"${potaConf.flink.localTmpDir}/${jobDef.namespace}@${jobDef.clusterId}/${getFileName(jobDef.jobJar)}")
        .map(_.getPath)

      // submit job
      _ <- logInfo(s"Start to submit job to flink cluster: \n${jobDef.toPrettyString}".stripMargin)
      jobId <- {
        for {
          _ <- logInfo(s"Uploading flink job jar to flink cluster, path: $jobJarPath, flink-rest: $restUrl")
          rest = flinkRest(restUrl)
          jarId <- rest.uploadJar(jobJarPath)
          jobId <- rest.runJar(jarId, RunJobReq(jobDef))
          _     <- rest.deleteJar(jarId).ignore
        } yield jobId
      }.mapError(err => RequestFlinkRestApiErr(err))

      _ <- lfs.rm(jobJarPath).ignore
      _ <- logInfo(s"Submit job to flink session cluster successfully, jobId: $jobId")
    } yield jobId
  } @@ ZIOAspect.annotated(Fcid(jobDef.clusterId, jobDef.namespace).toAnno: _*)

  /**
   * Cancel job in flink session cluster.
   */
  override def cancelSessJob(fjid: Fjid): IO[FlinkOprErr, Unit] = {
    for {
      restUrl <- flinkObserver.retrieveRestEndpoint(fjid.fcid, directly = true).map(_.chooseUrl)
      _       <- flinkRest(restUrl).cancelJob(fjid.jobId).mapError(RequestFlinkRestApiErr)
    } yield ()
  } @@ ZIOAspect.annotated(fjid.toAnno: _*)

  /**
   * Cancel job in flink application cluster.
   */
  override def cancelAppJob(fcid: Fcid): IO[FlinkOprErr, Unit] = {
    for {
      restUrl <- flinkObserver.retrieveRestEndpoint(fcid, directly = true).map(_.chooseUrl)
      jobId   <- findJobIdFromAppCluster(fcid)
      _       <- flinkRest(restUrl).cancelJob(jobId).mapError(RequestFlinkRestApiErr)
    } yield ()
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

  private def findJobIdFromAppCluster(fcid: Fcid): IO[FlinkOprErr, String] =
    flinkObserver
      .listJobIds(fcid)
      .map(_.headOption)
      .flatMap {
        case None        => fail(EmptyJobInCluster(fcid))
        case Some(jobId) => succeed(jobId)
      }
      .tap(jobId => logInfo(s"Found job-id in flink application cluster: $jobId "))

  /**
   * Stop job in flink session cluster with savepoint.
   */
  override def stopSessJob(fjid: Fjid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.retrieveRestEndpoint(fjid.fcid, directly = true).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(fjid.jobId, StopJobSptReq(savepoint)).mapError(RequestFlinkRestApiErr)
    } yield fjid -> triggerId
  } @@ ZIOAspect.annotated(fjid.toAnno: _*)

  /**
   * Cancel job in flink application cluster with savepoint.
   */
  override def stopAppJob(fcid: Fcid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.retrieveRestEndpoint(fcid, directly = true).map(_.chooseUrl)
      jobId     <- findJobIdFromAppCluster(fcid)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(jobId, StopJobSptReq(savepoint)).mapError(RequestFlinkRestApiErr)
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

  /**
   * Triggers a savepoint of flink session job.
   */
  override def triggerSessJobSavepoint(fjid: Fjid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.retrieveRestEndpoint(fjid.fcid, directly = true).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).triggerSavepoint(fjid.jobId, TriggerSptReq(savepoint)).mapError(RequestFlinkRestApiErr)
    } yield fjid -> triggerId
  } @@ ZIOAspect.annotated(fjid.toAnno: _*)

  /**
   * Triggers a savepoint of flink application job.
   */
  override def triggerAppJobSavepoint(fcid: Fcid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- flinkObserver.retrieveRestEndpoint(fcid, directly = true).map(_.chooseUrl)
      jobId     <- findJobIdFromAppCluster(fcid)
      triggerId <- flinkRest(restUrl).triggerSavepoint(jobId, TriggerSptReq(savepoint)).mapError(RequestFlinkRestApiErr)
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  override def killCluster(fcid: Fcid): IO[FlinkOprErr, Unit] = {
    k8sClient.apps.v1.deployments
      .delete(name = fcid.clusterId, namespace = fcid.namespace, deleteOptions = DeleteOptions())
      .mapError {
        case NotFound => ClusterNotFound(fcid)
        case failure  => FlinkOprErr.RequestK8sApiErr(failure)
      }
      .unit <*
    flinkObserver.unTrackCluster(fcid).ignore <*
    logInfo(s"Delete flink cluster successfully.")
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

}
