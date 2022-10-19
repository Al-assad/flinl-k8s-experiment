package kce.flink.operator.entity

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
    namespace: String,
    image: String,
    k8sAccount: String = KceConf.default.k8s.flinkAccount,
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

  def toFlinkRawConfig: Configuration = {
    EmptyConfiguration()
      .append("execution.target", "kubernetes-session")
      .append("kubernetes.cluster-id", clusterId)
      .append("kubernetes.namespace", namespace)
      .append("kubernetes.service-account", k8sAccount)
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
  }
}

object FlinkSessDef {
  implicit val jsonCodec: JsonCodec[FlinkSessDef] = DeriveJsonCodec.gen[FlinkSessDef]
}
