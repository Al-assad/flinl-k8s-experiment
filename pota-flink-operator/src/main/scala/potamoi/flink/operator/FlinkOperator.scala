package potamoi.flink.operator

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import potamoi.config.PotaConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import potamoi.flink.share.FlinkOprErr.ClusterNotFound
import potamoi.flink.share._
import potamoi.flink.share.model.FlinkExecMode.FlinkExecMode
import potamoi.flink.share.model._
import potamoi.fs.S3Operator
import potamoi.k8s.K8sClient
import potamoi.k8s._
import zio.ZIO.{logInfo, succeed}
import zio._

/**
 * Flink on Kubernetes Operator.
 */
trait FlinkOperator {
  def session: FlinkSessClusterOperator
  def application: FlinkAppClusterOperator
}

object FlinkOperator {
  val live = ZLayer {
    for {
      potaConf      <- ZIO.service[PotaConf]
      k8sOperator   <- ZIO.service[K8sOperator]
      s3Operator    <- ZIO.service[S3Operator]
      flinkObserver <- ZIO.service[FlinkObserver]
      sessClusterOpr = FlinkSessClusterOperatorImpl(potaConf, k8sOperator.client, s3Operator, flinkObserver)
      appClusterOpr  = FlinkAppClusterOperatorImpl(potaConf, k8sOperator.client, s3Operator, flinkObserver)
    } yield new FlinkOperator {
      val session     = sessClusterOpr
      val application = appClusterOpr
    }
  }
}

abstract class FlinkBaseOperator(potaConf: PotaConf, k8sClient: K8sClient, flinkObserver: FlinkObserver) {
  implicit val flkConf                 = potaConf.flink
  private val flinkClusterClientLoader = new DefaultClusterClientServiceLoader()

  // Local workplace directory for each Flink cluster.
  protected def clusterLocalWp(clusterId: String, namespace: String): UIO[String] =
    succeed(s"${potaConf.flink.localTmpDir}/${namespace}@${clusterId}")

  // Local Generated flink kubernetes pod-template file output path.
  protected def podTemplateFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWp(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/flink-podtemplate.yaml")

  // Local Generated flink kubernetes config file output path.
  protected def logConfFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWp(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/log-conf")

  // Get Flink ClusterClientFactory by execution mode.
  protected def getFlinkClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] = ZIO.attempt {
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    flinkClusterClientLoader.getClusterClientFactory(conf)
  }

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): FlinkIO[Unit] = {
    k8sClient.api.apps.v1.deployments
      .delete(name = fcid.clusterId, namespace = fcid.namespace, deleteOptions = DeleteOptions())
      .mapError {
        case NotFound => ClusterNotFound(fcid)
        case failure  => FlinkOprErr.RequestK8sApiErr(failure)
      }
      .unit <*
    flinkObserver.manager.untrackCluster(fcid).ignore <*
    logInfo(s"Delete flink cluster successfully.")
  } @@ ZIOAspect.annotated(fcid.toAnno: _*)
}
