package kce.flink.operator

import kce.flink.operator.FlinkConfigExtension.{EmptyConfiguration, configurationToPF}
import kce.flink.operator.entity.FlinkExecMode.FlinkExecMode
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
    val conf = EmptyConfiguration().append("execution.target", execMode.toString).value
    clusterClientLoader.getClusterClientFactory(conf)
  }

}
