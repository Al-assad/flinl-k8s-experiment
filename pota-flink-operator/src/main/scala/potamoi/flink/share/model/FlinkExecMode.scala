package potamoi.flink.share.model

import potamoi.common.ComplexEnum

object FlinkExecMode extends ComplexEnum {
  type FlinkExecMode = Value

  val K8sApp     = Value("kubernetes-application")
  val K8sSession = Value("kubernetes-session")
  val Unknown    = Value("Unknown")

  /**
   * infer execution mode from flink raw config value of "execution.target"
   */
  def ofRawConfValue(executionTarget: Option[String]): FlinkExecMode = executionTarget match {
    case Some("kubernetes-session") => K8sSession
    case Some("embedded")           => K8sApp
    case Some("remote")             => K8sSession
    case _                          => Unknown
  }
}
