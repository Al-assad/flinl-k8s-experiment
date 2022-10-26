package kce.flink.operator

import kce.common.ZIOExtension.{usingAttempt, usingAttemptBlocking}
import kce.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import kce.flink.operator.entity.FlinkExecMode.FlinkExecMode
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import org.apache.flink.client.program.ClusterClient
import zio.{Scope, Task, ZIO}

import scala.language.implicitConversions

/**
 * Helper function for Flink operation.
 */
object FlinkOprHelper {

  private val clusterClientLoader = new DefaultClusterClientServiceLoader()

  /**
   * Get Flink ClusterClientFactory by execution mode.
   */
  def getClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] = ZIO.attempt {
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    clusterClientLoader.getClusterClientFactory(conf)
  }

  /**
   * Get Flink RestClient.
   */
  def getFlinkRestClient(execMode: FlinkExecMode, clusterId: String, namespace: String): ZIO[Scope, ClusterNotFound, ClusterClient[String]] = {
    for {
      clusterClientFactory <- getClusterClientFactory(execMode)
      clusterClientDescriptor <- usingAttempt {
        clusterClientFactory.createClusterDescriptor(
          EmptyConfiguration()
            .append("execution.target", execMode)
            .append("kubernetes.cluster-id", clusterId)
            .append("kubernetes.namespace", namespace)
            .value
        )
      }
      clusterClient <- usingAttemptBlocking(clusterClientDescriptor.retrieve(clusterId).getClusterClient)
    } yield clusterClient
  }.mapError(ClusterNotFound(clusterId, namespace, _))

}
