package potamoi.flink.operator

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import potamoi.common.PathTool.{getFileName, isS3Path}
import potamoi.common.PrettyPrintable
import potamoi.common.ZIOExtension.usingAttempt
import potamoi.conf.PotaConf
import potamoi.flink.observer.FlinkK8sObserver
import potamoi.flink.operator.FlinkConfigExtension.configurationToPF
import potamoi.flink.operator.FlinkK8sOperator.getClusterClientFactory
import potamoi.flink.operator.FlinkOprErr._
import potamoi.flink.operator.FlinkRestRequest.{RunJobReq, StopJobSptReq}
import potamoi.flink.share.FlinkExecMode.K8sSession
import potamoi.flink.share._
import potamoi.fs.{lfs, S3Operator}
import potamoi.k8s.stringToK8sNamespace
import zio.ZIO.{attempt, attemptBlockingInterrupt, logDebug, logInfo, scoped, succeed}
import zio._

import scala.language.implicitConversions

/**
 * Default FlinkK8sOperator implementation.
 */
class FlinkK8sOperatorLive(potaConf: PotaConf, k8sClient: Kubernetes, s3Operator: S3Operator, flinkObserver: FlinkK8sObserver)
    extends FlinkK8sOperator {

  private val clDefResolver   = ClusterDefResolver
  private val podTplResolver  = PodTemplateResolver
  private val logConfResolver = LogConfigResolver
  private val flinkRest       = FlinkRestRequest

  implicit val flinkConf = potaConf.flink

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
   * Deploy Flink Application cluster.
   */
  override def deployApplicationCluster(clusterDef: FlinkAppClusterDef): IO[FlinkOprErr, Unit] = {
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
          clusterClientFactory <- getClusterClientFactory(K8sSession)
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
  override def deploySessionCluster(clusterDef: FlinkSessClusterDef): IO[FlinkOprErr, Unit] = {
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
          clusterClientFactory <- getClusterClientFactory(K8sSession)
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
  override def cancelSessionJob(fjid: Fjid, savepoint: FlinkJobSptConf): IO[FlinkOprErr, Option[TriggerId]] = {
    for {
      restUrl <- flinkObserver.retrieveRestEndpoint(fjid.fcid, directly = true).map(_.chooseUrl)
      result  <- cancelJob(restUrl, fjid.jobId, savepoint)
    } yield result
  } @@ ZIOAspect.annotated(fjid.toAnno: _*)

  /**
   * Cancel job in flink application cluster.
   */
  override def cancelApplicationJob(fcid: Fcid, savepoint: FlinkJobSptConf): IO[FlinkOprErr, Option[TriggerId]] = {
    for {
      restUrl  <- flinkObserver.retrieveRestEndpoint(fcid, directly = true).map(_.chooseUrl)
      jobIdOpt <- flinkObserver.listJobIds(fcid).map(_.headOption)
      _        <- logDebug(s"Found jobId for flink application cluster: $jobIdOpt").when(jobIdOpt.isDefined)
      result <- jobIdOpt match {
        case None        => succeed(None)
        case Some(jobId) => cancelJob(restUrl, jobId, savepoint)
      }
    } yield result
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)

  private def cancelJob(restUrl: String, jobId: String, savepoint: FlinkJobSptConf): IO[FlinkOprErr, Option[TriggerId]] = {
    if (!savepoint.enable) flinkRest(restUrl).cancelJob(jobId).as(None)
    else {
      val req = StopJobSptReq(drain = savepoint.drain, formatType = savepoint.formatType, targetDirectory = savepoint.savepointPath)
      flinkRest(restUrl).stopJobWithSavepoint(jobId, req).map(Some(_))
    }
  }.mapError(RequestFlinkRestApiErr)

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
