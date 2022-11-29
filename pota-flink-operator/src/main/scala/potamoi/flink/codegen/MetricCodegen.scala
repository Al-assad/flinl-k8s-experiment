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
    getMetrics = restReq.getJmMetrics
  ).map(println)
}
