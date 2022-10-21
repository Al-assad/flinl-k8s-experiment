package kce.flink.operator.entity

import kce.conf.KceConf
import org.apache.flink.configuration.Configuration
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink K8s session cluster definition.
 */
case class FlinkSessClusterDef(
    flinkVer: FlinkVer,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None,
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
    extends FlinkClusterDefinition[FlinkSessClusterDef] {

  val mode = FlinkExecMode.K8sSession

  /**
   * Whether built-in s3 storage support is required.
   */
  def isS3Required: Boolean = checkS3Required()

  /**
   * Convert to Flink raw configuration.
   */
  def toFlinkRawConfig(kceConf: KceConf): Configuration = convertToFlinkConfig(kceConf, identity)

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def revise(): FlinkSessClusterDef = reviseDefinition(this)

  protected def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkSessClusterDef = copy(extRawConfigs = extRawConfigs)
  protected def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkSessClusterDef       = copy(builtInPlugins = builtInPlugins)

}

object FlinkSessClusterDef {
  implicit val jsonCodec: JsonCodec[FlinkSessClusterDef] = DeriveJsonCodec.gen[FlinkSessClusterDef]
}
