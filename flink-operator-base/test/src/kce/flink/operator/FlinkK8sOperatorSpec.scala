package kce.flink.operator

import kce.common.valueToSome
import kce.conf.{K8sClient, PotaConf}
import kce.flink.operator.share.CheckpointStorageType.Filesystem
import kce.flink.operator.share.StateBackendType.Rocksdb
import kce.flink.operator.share._
import kce.fs.S3Operator
import kce.testkit.STSpec

// TODO unsafe submit
class FlinkK8sOperatorSpec extends STSpec {

  val layers = (PotaConf.live >+> K8sClient.live) >+> S3Operator.live >>> FlinkK8sOperator.live

  "retrieveRestEndpoint" in {
    FlinkK8sOperator
      .retrieveRestEndpoint("session-01", "fdev")
      .provide(layers)
      .debugStack
      .run
  }

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
        .debugStack
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
        .debugStack
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
        .debugStack
        .run
    }
  }

  "deploy application cluster" should {
    "normal" in {
      FlinkK8sOperator
        .deployApplicationCluster(
          FlinkAppClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "app-t1",
            namespace = "fdev",
            image = "flink:1.15.2",
            jobJar = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
          ))
        .provide(layers)
        .debugStack
        .run
    }
    "with user jar on s3" in {
      FlinkK8sOperator
        .deployApplicationCluster(
          FlinkAppClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "app-t2",
            namespace = "fdev",
            image = "flink:1.15.2",
            jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
          ))
        .provide(layers)
        .debugStack
        .run
    }
    "with state backend" in {
      FlinkK8sOperator
        .deployApplicationCluster(
          FlinkAppClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "app-t3",
            namespace = "fdev",
            image = "flink:1.15.2",
            jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar",
            stateBackend = StateBackendConf(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            )
          ))
        .provide(layers)
        .debugStack
        .run
    }

    "submit job to session cluster" in {
      FlinkK8sOperator
        .submitJobToSession(
          FlinkSessJobDef(
            clusterId = "session-01",
            namespace = "fdev",
            jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
          )
        )
        .provide(layers)
        .debugStack
        .run
    }
  }

  "kill flink cluster" in {
    FlinkK8sOperator
      .killCluster("app-t1", "fdev")
      .provide(layers)
      .debugStack
      .run
  }

}
