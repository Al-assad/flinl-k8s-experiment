package potamoi.flink.codegen

import potamoi.flink.operator.flinkRest
import zio.ZIOAppDefault

/**
 * Code generator for FlinkJmMetrics.
 */
private object JmMetricCodegen extends ZIOAppDefault {
  val restReq = flinkRest("http://10.233.46.104:8081")
  val run = genMetricCode(
    caseClzName = "FlinkJmMetrics",
    listKeys = restReq.getJmMetricsKeys,
    getMetrics = restReq.getJmMetrics,
    caseClzExtFields = Set(
      "clusterId" -> "String",
      "namespace" -> "String"
    ),
    fromRawFuncExtParams = "fcid: Fcid",
    fromRawFucExtFieldsFill = s"""clusterId = fcid.clusterId,
                                 |namespace = fcid.namespace,""".stripMargin
  ).map(println)
}

private object TmMetricCodegen extends ZIOAppDefault {
  val restReq = flinkRest("http://10.233.46.104:8081")
  val run = genMetricCode(
    caseClzName = "FlinkTmMetrics",
    listKeys = restReq.getTmMetricsKeys("session-01-taskmanager-1-45"),
    getMetrics = restReq.getTmMetrics("session-01-taskmanager-1-45", _),
    caseClzExtFields = Set(
      "clusterId" -> "String",
      "namespace" -> "String",
      "tid"       -> "String"
    ),
    fromRawFuncExtParams = "ftid: Ftid",
    fromRawFucExtFieldsFill = s"""clusterId = ftid.clusterId,
                                 |namespace = ftid.namespace,
                                 |tid = ftid.tid,
                                 |""".stripMargin
  ).map(println)
}
