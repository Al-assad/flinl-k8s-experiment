package potamoi.flink.operator

import potamoi.cluster.PotaActorSystem
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.share.model.CheckpointStorageType.Filesystem
import potamoi.flink.share.model.StateBackendType.Rocksdb
import potamoi.flink.share.model._
import potamoi.fs.S3Operator
import potamoi.k8s.K8sClient
import potamoi.syntax.valueToSome
import potamoi.testkit.{PotaDev, STSpec, UnsafeEnv}
import zio.{IO, ZIO}

// todo unsafe
class FlinkK8sOperatorSpec extends STSpec {

  private def testOpr[E, A](effect: FlinkOperator => IO[E, A]): Unit = {
    ZIO
      .serviceWithZIO[FlinkOperator](effect(_))
      .provide(
        PotaDev.conf,
        PotaActorSystem.live,
        K8sClient.live,
        S3Operator.live,
        FlinkObserver.live,
        FlinkOperator.live
      )
      .runSpec
  }

  "deploy session cluster" should {
    "normal" taggedAs UnsafeEnv in testOpr { opr =>
      opr.session
        .deployCluster(
          FlinkSessClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "session-t1",
            namespace = "fdev",
            image = "flink:1.15.2"
          ))
        .debug
    }

    "with state backend" taggedAs UnsafeEnv in testOpr { opr =>
      opr.session
        .deployCluster(
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
        .debug
    }
    "with state backend and extra dependencies" taggedAs UnsafeEnv in testOpr { opr =>
      opr.session
        .deployCluster(
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
        .debug
    }
  }

  "deploy application cluster" should {
    "normal" taggedAs UnsafeEnv in testOpr { opr =>
      opr.application
        .deployCluster(
          FlinkAppClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "app-t1",
            namespace = "fdev",
            image = "flink:1.15.2",
            jobJar = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
          ))
        .debug
    }
    "with user jar on s3" taggedAs UnsafeEnv in testOpr { opr =>
      opr.application
        .deployCluster(
          FlinkAppClusterDef(
            flinkVer = FlinkVer("1.15.2"),
            clusterId = "app-t2",
            namespace = "fdev",
            image = "flink:1.15.2",
            jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
          ))
        .debug
    }
    "with state backend" taggedAs UnsafeEnv in testOpr { opr =>
      opr.application
        .deployCluster(
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
        .debug
    }
    "submit job to session cluster" taggedAs UnsafeEnv in testOpr { opr =>
      opr.session
        .submitJob(
          FlinkSessJobDef(
            clusterId = "session-01",
            namespace = "fdev",
            jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
          ))
        .debug
    }
  }

  "kill flink cluster" taggedAs UnsafeEnv in testOpr { opr =>
    opr.application.killCluster("app-t1", "fdev").debug
  }

}
