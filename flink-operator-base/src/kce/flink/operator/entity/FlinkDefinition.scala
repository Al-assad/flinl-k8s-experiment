package kce.flink.operator.entity

import kce.common.S3Tool.isS3Path
import kce.conf.KceConf
import kce.flink.operator.FlinkConfigExtension.{configurationToPF, EmptyConfiguration}
import org.apache.flink.configuration.Configuration
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.language.implicitConversions

/**
 * Flink session cluster definition.
 */
case class FlinkSessDef(
    clusterId: String,
    namespace: String = "default",
    flinkVer: FlinkVer,
    image: String,
    k8sAccount: Option[String] = None,
    cpu: CpuConf = CpuConf(),
    mem: MemConf = MemConf(),
    par: ParConf = ParConf(),
    webui: WebUIConf = WebUIConf(enableSubmit = true, enableCancel = true),
    restartStg: RestartStgConf = NonRestartStg,
    stateBackend: Option[StateBackendConf] = None,
    jmHa: Option[JmHaConf] = None,
    injectedDeps: List[String] = List.empty,
    builtInPlugins: List[String] = List.empty,
    overridePodTemplate: Option[String] = None,
    extRawConfigs: Map[String, String] = Map.empty) {

  /**
   * Convert to Flink raw configuration.
   */
  def toFlinkRawConfig(kceConf: KceConf): Configuration = {
    EmptyConfiguration()
      .append("execution.target", "kubernetes-session")
      .append("kubernetes.cluster-id", clusterId)
      .append("kubernetes.namespace", namespace)
      .append("kubernetes.service-account", k8sAccount.getOrElse(kceConf.flink.k8sAccount))
      // inner config settings
      .append(cpu)
      .append(mem)
      .append(par)
      .append(webui)
      .append(restartStg)
      .append(stateBackend)
      .append(jmHa)
      // plugins settings
      .pipeWhen(builtInPlugins.nonEmpty) { conf =>
        conf
          .append("containerized.master.env.ENABLE_BUILT_IN_PLUGINS", builtInPlugins)
          .append("containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS", builtInPlugins)
      }
      // override extra raw configs
      .merge(extRawConfigs)
      .append("$internal.deployment.config-dir", kceConf.flink.localLogConfDir)
  }

  /**
   * Whether built-in s3 storage support is required.
   */
  def isS3Required: Boolean = {
    lazy val checkStateBackend = stateBackend.exists { c =>
      if (c.checkpointDir.exists(isS3Path)) true
      else c.savepointDir.exists(isS3Path)
    }
    lazy val checkJmHa         = jmHa.exists(c => isS3Path(c.storageDir))
    lazy val checkInjectDeps   = injectedDeps.exists(isS3Path)
    lazy val checkExRawConfigs = extRawConfigs.exists { case (_, v) => isS3Path(v) }

    if (checkStateBackend) true
    else if (checkJmHa) true
    else if (checkInjectDeps) true
    else checkExRawConfigs
  }

}

object FlinkSessDef {
  implicit val jsonCodec: JsonCodec[FlinkSessDef] = DeriveJsonCodec.gen[FlinkSessDef]
}
