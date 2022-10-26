package kce.flink.operator.entity

/**
 * Definition of the job submitted to Flink session cluster.
 */
case class FlinkSessJobDef(
    clusterId: String,
    namespace: String,
    jobJar: String,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    parallelism: Option[Int] = None,
    savepointPath: Option[String] = None,
    allowNonRestoredState: Option[Boolean] = None)
