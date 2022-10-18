package kce.flink.operator

import kce.common.ComplexEnum

object FlinkExecMode extends ComplexEnum {
  type FlinkExecMode = Value
  val K8sApp, K8sSession = Value
}
