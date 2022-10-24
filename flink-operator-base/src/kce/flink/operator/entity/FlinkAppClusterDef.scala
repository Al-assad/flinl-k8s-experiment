package kce.flink.operator.entity

import cats.Eval.later
import kce.common.S3Tool.isS3Path
import kce.conf.KceConf
import kce.flink.operator.entity.RestExportType.RestExportType
import org.apache.flink.configuration.Configuration
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink K8s application cluster definition.
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
    extends FlinkClusterDefinition[FlinkAppClusterDef] {

  val mode = FlinkExecMode.K8sApp

  /**
   * Whether built-in s3 storage support is required.
   */
  def isS3Required: Boolean = checkS3Required(later(isS3Path(jobJar)))

  /**
   * Convert to Flink raw configuration.
   */
  def toFlinkRawConfig(kceConf: KceConf): Configuration = convertToFlinkConfig(
    kceConf,
    moreInject = { conf =>
      // when jobJar path is s3 path, replace with pvc local path.
      val reviseJarPath = if (isS3Path(jobJar)) s"file:///opt/flink/lib/${jobJar.split('/').last}" else jobJar
      conf
        .append("pipeline.jars", reviseJarPath)
        .append("pipeline.name", jobName)
    }
  )

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def revise(): FlinkAppClusterDef = reviseDefinition()

  protected def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkAppClusterDef = copy(extRawConfigs = extRawConfigs)
  protected def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkAppClusterDef       = copy(builtInPlugins = builtInPlugins)
}

object FlinkAppClusterDef {
  implicit val jsonCodec: JsonCodec[FlinkAppClusterDef] = DeriveJsonCodec.gen[FlinkAppClusterDef]
}
