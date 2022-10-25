package kce.flink.operator

import kce.common.valueToSome
import kce.conf.{K8sClient, KceConf}
import kce.flink.operator.entity.CheckpointStorageType.Filesystem
import kce.flink.operator.entity.StateBackendType.Rocksdb
import kce.flink.operator.entity.{FlinkSessClusterDef, FlinkVer, StateBackendConf}
import kce.testkit.STSpec

class FlinkK8sOperatorSpec extends STSpec {

  val layers = (KceConf.live >+> K8sClient.live) >+> FlinkK8sOperator.live

  // TODO unsafe
  "retrieveRestEndpoint" in {
    FlinkK8sOperator
      .retrieveRestEndpoint("session-01", "fdev")
      .provide(layers)
      .debug
      .run
  }

  // TODO unsafe
  "deploy session cluster" should {
    "normal" in {
      FlinkK8sOperator
        .deploySessionCluster(
          FlinkSessClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "session-t1",
            namespace = "fdev",
            image = "flink:1.15.2"
          ))
        .provide(layers)
        .debug
        .run
    }
    "with state backend " in {
      FlinkK8sOperator
        .deploySessionCluster(
          FlinkSessClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "session-t2",
            namespace = "fdev",
            image = "flink:1.15.2",
            stateBackend = StateBackendConf(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            )
          ))
        .provide(layers)
        .debug
        .run
    }
    "with state backend and extra dependencies" in {
      FlinkK8sOperator
        .deploySessionCluster(
          FlinkSessClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "session-t3",
            namespace = "fdev",
            image = "flink:1.15.2",
            stateBackend = StateBackendConf(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            ),
            injectedDeps = Set(
              "s3://flink-dev/flink-connector-jdbc-1.15.2.jar",
              "s3://flink-dev/mysql-connector-java-8.0.30.jar"
            )
          ))
        .provide(layers)
        .debug
        .run
    }
  }

  "deploy application cluster" in {

  }

}
