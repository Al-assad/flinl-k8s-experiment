package kce.flink.operator

import FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import kce.flink.operator.entity.FlinkExecMode.{FlinkExecMode, K8sApp, K8sSession}
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import zio.{Task, ZIO}

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
    val conf = execMode match {
      case K8sSession => EmptyConfiguration().append("execution.target", "kubernetes-session")
      case K8sApp     => EmptyConfiguration().append("execution.target", "kubernetes-application")
    }
    clusterClientLoader.getClusterClientFactory(conf)
  }

}
