package potamoi.flink.operator

import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import potamoi.conf.PotaConf
import potamoi.flink.observer.FlinkK8sObserver
import potamoi.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import potamoi.flink.share.FlinkExecMode.FlinkExecMode
import potamoi.flink.share._
import potamoi.fs.S3Operator
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
  def deployAppCluster(definition: FlinkAppClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessCluster(definition: FlinkSessClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Submit job to Flink session cluster.
   */
  def submitJobToSession(definition: FlinkSessJobDef): IO[FlinkOprErr, JobId]

  /**
   * Cancel job in flink session cluster.
   */
  def cancelSessJob(fjid: Fjid): IO[FlinkOprErr, Unit]

  /**
   * Cancel job in flink application cluster.
   */
  def cancelAppJob(fcid: Fcid): IO[FlinkOprErr, Unit]

  /**
   * Stop job in flink session cluster with savepoint.
   */
  def stopSessJob(fjid: Fjid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)]

  /**
   * Cancel job in flink application cluster with savepoint.
   */
  def stopAppJob(fcid: Fcid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink session job.
   */
  def triggerSessJobSavepoint(fjid: Fjid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink application job.
   */
  def triggerAppJobSavepoint(fcid: Fcid, savepoint: FlinkJobSptDef): IO[FlinkOprErr, (Fjid, TriggerId)]

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): IO[FlinkOprErr, Unit]

}

object FlinkK8sOperator {

  val live = ZLayer {
    for {
      potaConf      <- ZIO.service[PotaConf]
      k8sClient     <- ZIO.service[Kubernetes]
      s3Operator    <- ZIO.service[S3Operator]
      flinkObserver <- ZIO.service[FlinkK8sObserver]
    } yield new FlinkK8sOperatorLive(potaConf, k8sClient, s3Operator, flinkObserver)
  }

  private val clusterClientLoader = new DefaultClusterClientServiceLoader()

  /**
   * Get Flink ClusterClientFactory by execution mode.
   */
  def getClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] = ZIO.attempt {
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    clusterClientLoader.getClusterClientFactory(conf)
  }

}
