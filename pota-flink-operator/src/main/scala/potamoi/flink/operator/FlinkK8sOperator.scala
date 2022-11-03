package potamoi.flink.operator

import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import potamoi.conf.PotaConf
import potamoi.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import potamoi.flink.operator.FlinkK8sOperator.{JobId, SavepointTriggerId}
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
  def deployApplicationCluster(definition: FlinkAppClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Deploy Flink session cluster.
   */
  def deploySessionCluster(definition: FlinkSessClusterDef): IO[FlinkOprErr, Unit]

  /**
   * Submit job to Flink session cluster.
   */
  def submitJobToSession(definition: FlinkSessJobDef): IO[FlinkOprErr, JobId]

  /**
   * Cancel job in flink session cluster.
   */
  def cancelSessionJob(fcid: Fcid, jobId: String, savepoint: FlinkJobSptConf): IO[FlinkOprErr, Option[SavepointTriggerId]]

  /**
   * Cancel job in flink application cluster.
   */
  def cancelApplicationJob(fcid: Fcid, savepoint: FlinkJobSptConf): IO[FlinkOprErr, Option[SavepointTriggerId]]

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): IO[FlinkOprErr, Unit]

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieveRestEndpoint(fcid: Fcid): IO[FlinkOprErr, FlinkRestSvcEndpoint]
}

object FlinkK8sOperator {

  val live: ZLayer[S3Operator with Kubernetes with PotaConf, Nothing, FlinkK8sOperator] = ZLayer {
    for {
      potaConf   <- ZIO.service[PotaConf]
      k8sClient  <- ZIO.service[Kubernetes]
      s3Operator <- ZIO.service[S3Operator]
    } yield new FlinkK8sOperatorLive(potaConf, k8sClient, s3Operator)
  }

  type JobId              = String
  type SavepointTriggerId = String

  private val clusterClientLoader = new DefaultClusterClientServiceLoader()

  /**
   * Get Flink ClusterClientFactory by execution mode.
   */
  def getClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] = ZIO.attempt {
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    clusterClientLoader.getClusterClientFactory(conf)
  }

}
