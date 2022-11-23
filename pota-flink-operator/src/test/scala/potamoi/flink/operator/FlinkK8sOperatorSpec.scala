package potamoi.flink.operator

import potamoi.cluster.PotaActorSystem
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.share.model.CheckpointStorageType.Filesystem
import potamoi.flink.share.model.StateBackendType.Rocksdb
import potamoi.flink.share.model._
import potamoi.fs.S3Operator
import potamoi.k8s.K8sClient
import potamoi.logger.PotaLogger
import potamoi.syntax.valueToSome
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.ZIO

// todo unsafe
class FlinkK8sOperatorSpec extends STSpec {

  val layers = {
    PotaDev.conf >+>
    PotaLogger.live ++ K8sClient.live ++ PotaActorSystem.live >+>
    S3Operator.live ++ FlinkObserver.live >>>
    FlinkOperator.live
  }

  "deploy session cluster" should {
    "normal" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.session.deployCluster(
            FlinkSessClusterDef(
              flinkVer = FlinkVer("1.15.2"),
              clusterId = "session-t1",
              namespace = "fdev",
              image = "flink:1.15.2"
            )
          )
        }
        .provide(layers)
        .debug
        .run
    }
    "with state backend" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.session.deployCluster(
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
            )
          )
        }
        .provide(layers)
        .debug
        .run
    }
    "with state backend and extra dependencies" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.session.deployCluster(
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
            )
          )
        }
        .provide(layers)
        .debug
        .run
    }
  }

  "deploy application cluster" should {
    "normal" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.application.deployCluster(
            FlinkAppClusterDef(
              flinkVer = FlinkVer("1.15.2"),
              clusterId = "app-t1",
              namespace = "fdev",
              image = "flink:1.15.2",
              jobJar = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
            )
          )
        }
        .provide(layers)
        .debug
        .runSpec
    }
    "with user jar on s3" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.application.deployCluster(
            FlinkAppClusterDef(
              flinkVer = FlinkVer("1.15.2"),
              clusterId = "app-t2",
              namespace = "fdev",
              image = "flink:1.15.2",
              jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
            ))
        }
        .provide(layers)
        .debug
        .run
    }
    "with state backend" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.application.deployCluster(
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
        }
        .provide(layers)
        .debug
        .run
    }

    "submit job to session cluster" taggedAs UnsafeEnv in {
      ZIO
        .serviceWithZIO[FlinkOperator] { opr =>
          opr.session.submitJob(
            FlinkSessJobDef(
              clusterId = "session-01",
              namespace = "fdev",
              jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
            )
          )
        }
        .provide(layers)
        .debug
        .run
    }
  }

  "kill flink cluster" taggedAs UnsafeEnv in {
    ZIO
      .serviceWithZIO[FlinkOperator] { opr =>
        opr.application.killCluster("app-t1", "fdev")
      }
      .provide(layers)
      .debug
      .run
  }

}
