package kce.flink.operator.share

import kce.flink.operator.share.FlinkExecMode.FlinkExecMode
import kce.flink.operator.share.RestExportType.RestExportType
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.language.implicitConversions

/**
 * Flink Kubernetes cluster definition.
 */
sealed trait FlinkClusterDef[SubType <: FlinkClusterDef[SubType]] { this: SubType =>

  val flinkVer: FlinkVer
  val clusterId: String
  val namespace: String
  val image: String
  val k8sAccount: Option[String]
  val restExportType: RestExportType

  val cpu: CpuConf
  val mem: MemConf
  val par: ParConf
  val webui: WebUIConf
  val restartStg: RestartStgConf
  val stateBackend: Option[StateBackendConf]
  val jmHa: Option[JmHaConf]
  val s3: Option[S3AccessConf]

  val injectedDeps: Set[String]
  val builtInPlugins: Set[String]
  val extRawConfigs: Map[String, String]
  val overridePodTemplate: Option[String]

  val mode: FlinkExecMode

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): SubType
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): SubType
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConf]): SubType
  private[flink] def copyJmHa(jmHa: Option[JmHaConf]): SubType
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): SubType
}

object FlinkClusterDef {
  import FlinkRawConf._
  implicit val sessClusterDefCodec: JsonCodec[FlinkSessClusterDef] = DeriveJsonCodec.gen[FlinkSessClusterDef]
  implicit val appClusterDefCodec: JsonCodec[FlinkAppClusterDef]   = DeriveJsonCodec.gen[FlinkAppClusterDef]
}

/**
 * Flink K8s session cluster definition.
 */
case class FlinkSessClusterDef(
    flinkVer: FlinkVer,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None,
    restExportType: RestExportType = RestExportType.ClusterIP,
    cpu: CpuConf = CpuConf(),
    mem: MemConf = MemConf(),
    par: ParConf = ParConf(),
    webui: WebUIConf = WebUIConf(enableSubmit = true, enableCancel = true),
    restartStg: RestartStgConf = NonRestartStg,
    stateBackend: Option[StateBackendConf] = None,
    jmHa: Option[JmHaConf] = None,
    s3: Option[S3AccessConf] = None,
    injectedDeps: Set[String] = Set.empty,
    builtInPlugins: Set[String] = Set.empty,
    extRawConfigs: Map[String, String] = Map.empty,
    overridePodTemplate: Option[String] = None)
    extends FlinkClusterDef[FlinkSessClusterDef] {

  val mode = FlinkExecMode.K8sSession

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkSessClusterDef    = copy(extRawConfigs = extRawConfigs)
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkSessClusterDef          = copy(builtInPlugins = builtInPlugins)
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConf]): FlinkSessClusterDef = copy(stateBackend = stateBackend)
  private[flink] def copyJmHa(jmHa: Option[JmHaConf]): FlinkSessClusterDef                         = copy(jmHa = jmHa)
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): FlinkSessClusterDef                = copy(injectedDeps = injectedDeps)
}

/**
 * Flink K8s application cluster definition.
 * @param jobJar Flink job jar path, supports local file or s3 path.
 */
case class FlinkAppClusterDef(
    flinkVer: FlinkVer,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None,
    restExportType: RestExportType = RestExportType.ClusterIP,
    jobJar: String,
    jobName: Option[String] = None,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    cpu: CpuConf = CpuConf(),
    mem: MemConf = MemConf(),
    par: ParConf = ParConf(),
    webui: WebUIConf = WebUIConf(enableSubmit = false, enableCancel = false),
    restartStg: RestartStgConf = NonRestartStg,
    stateBackend: Option[StateBackendConf] = None,
    jmHa: Option[JmHaConf] = None,
    s3: Option[S3AccessConf] = None,
    injectedDeps: Set[String] = Set.empty,
    builtInPlugins: Set[String] = Set.empty,
    extRawConfigs: Map[String, String] = Map.empty,
    overridePodTemplate: Option[String] = None)
    extends FlinkClusterDef[FlinkAppClusterDef] {

  val mode = FlinkExecMode.K8sApp

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkAppClusterDef    = copy(extRawConfigs = extRawConfigs)
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkAppClusterDef          = copy(builtInPlugins = builtInPlugins)
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConf]): FlinkAppClusterDef = copy(stateBackend = stateBackend)
  private[flink] def copyJmHa(jmHa: Option[JmHaConf]): FlinkAppClusterDef                         = copy(jmHa = jmHa)
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): FlinkAppClusterDef                = copy(injectedDeps = injectedDeps)
}
