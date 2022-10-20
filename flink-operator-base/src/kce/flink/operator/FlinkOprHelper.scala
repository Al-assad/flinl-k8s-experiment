package kce.flink.operator

import kce.flink.operator.FlinkConfigExtension.{EmptyConfiguration, configurationToPF}
import kce.flink.operator.FlinkExecMode.{FlinkExecMode, K8sApp, K8sSession}
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}

import scala.language.implicitConversions

object FlinkOprHelper {

  private val clusterClientLoader = new DefaultClusterClientServiceLoader()

  /**
   * Get Flink ClusterClientFactory by execution mode.
   */
  def clusterClientFactory(execMode: FlinkExecMode): ClusterClientFactory[String] = {
    val conf = execMode match {
      case K8sSession => EmptyConfiguration().append("execution.target", "kubernetes-session")
      case K8sApp     => EmptyConfiguration().append("execution.target", "kubernetes-application")
    }
    clusterClientLoader.getClusterClientFactory(conf)
  }

}


