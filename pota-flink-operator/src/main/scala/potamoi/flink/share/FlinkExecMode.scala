package potamoi.flink.share

import potamoi.common.ComplexEnum

object FlinkExecMode extends ComplexEnum {
  type FlinkExecMode = Value

  val K8sApp     = Value("kubernetes-application")
  val K8sSession = Value("kubernetes-session")
  val Unknown    = Value("Unknown")
}
