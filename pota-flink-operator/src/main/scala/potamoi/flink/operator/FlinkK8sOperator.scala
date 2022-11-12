package potamoi.flink.operator

import potamoi.flink.share._
import zio._
import zio.macros.accessible

/**
 * Flink on Kubernetes Operator.
 */
@accessible
trait FlinkK8sOperator {

  /**
   * Deploy Flink Application cluster.
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
