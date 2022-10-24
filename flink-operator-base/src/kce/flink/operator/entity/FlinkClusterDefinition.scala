package kce.flink.operator.entity

import cats.Eval
import kce.common.CollectionExtension.StringIterableWrapper
import kce.common.PathTool.isS3Path
import kce.common.{safeTrim, ComplexEnum, GenericPF}
import kce.conf.KceConf
import kce.flink.operator.FlinkConfigExtension.{configurationToPF, ConfigurationPF, EmptyConfiguration}
import kce.flink.operator.FlinkPlugins
import kce.flink.operator.FlinkPlugins.defaultS3Plugin
import kce.flink.operator.entity.FlinkClusterDefinition.notAllowCustomRawConfKeys
import kce.flink.operator.entity.FlinkExecMode.FlinkExecMode
import kce.flink.operator.entity.RestExportType.RestExportType
import org.apache.flink.configuration.Configuration

import scala.language.implicitConversions

/**
 * Flink Kubernetes cluster definition.
 */
trait FlinkClusterDefinition[SubType <: FlinkClusterDefinition[SubType]] { this: SubType =>

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

  /**
   * Execution mode.
   */
  val mode: FlinkExecMode

  /**
   *  Whether built-in s3 storage support is required.
   */
  def isS3Required: Boolean

  /**
   * Check whether S3 support is required.
   */
  protected def checkS3Required(moreChecks: Eval[Boolean] = Eval.now(false)): Boolean = {
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
    else if (checkExRawConfigs) true
    else moreChecks.value
  }

  /**
   * Convert to Flink raw configuration.
   */
  protected def convertToFlinkConfig(kceConf: KceConf, moreInject: ConfigurationPF => ConfigurationPF): Configuration = {
    EmptyConfiguration()
      // inject inner raw configs
      .append("execution.target", mode.toString)
      .append("kubernetes.cluster-id", clusterId)
      .append("kubernetes.namespace", namespace)
      .append("kubernetes.container.image", image)
      .append("kubernetes.jobmanager.service-account", k8sAccount.getOrElse(kceConf.flink.k8sAccount))
      .append("kubernetes.rest-service.exposed.type", restExportType.toString)
      .append("blob.server.port", 6124)
      .append("taskmanager.rpc.port", 6122)
      .append(cpu)
      .append(mem)
      .append(par)
      .append(webui)
      .append(restartStg)
      .append(stateBackend)
      .append(jmHa)
      // s3 raw configs if necessary
      .pipe { conf =>
        s3 match {
          case Some(c)              => c.injectRaw(conf)
          case None if isS3Required => S3AccessConf(kceConf.s3).injectRaw(conf)
          case _                    => conf
        }
      }
      // built-in plugins raw configs
      .pipeWhen(builtInPlugins.nonEmpty) { conf =>
        conf
          .append("containerized.master.env.ENABLE_BUILT_IN_PLUGINS", builtInPlugins)
          .append("containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS", builtInPlugins)
      }
      .pipe(moreInject)
      // override extra raw configs
      .merge(extRawConfigs)
  }

  protected type RevisePipe = SubType => SubType

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def revise(): SubType

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  protected def reviseDefinition(): SubType = {
    val removeNotAllowCustomRawConfigs: RevisePipe = _.copyExtRawConfigs(
      extRawConfigs
        .map(kv => safeTrim(kv._1) -> safeTrim(kv._2))
        .filter(kv => kv._1.nonEmpty && kv._2.nonEmpty)
        .filter(kv => !notAllowCustomRawConfKeys.contains(kv._1))
    )
    val completeBuiltInPlugins: RevisePipe = _.copyBuiltInPlugins(
      builtInPlugins
        .filterNotBlank()
        .map { name =>
          FlinkPlugins.plugins.find(_.name == name) match {
            case None         => name
            case Some(plugin) => plugin.jarName(flinkVer)
          }
        }
        .toSet
    )
    val ensureS3Plugins: RevisePipe = { definition =>
      lazy val s3PluginReady = FlinkPlugins.s3Plugins
        .map(_.jarName(flinkVer))
        .contra { optJarNames =>
          (definition.builtInPlugins & optJarNames).nonEmpty
        }
      if (isS3Required && !s3PluginReady) definition.copyBuiltInPlugins(builtInPlugins + defaultS3Plugin.name)
      else definition
    }
    val ensureHdfsPlugins: RevisePipe = identity

    val pipe = removeNotAllowCustomRawConfigs andThen completeBuiltInPlugins andThen ensureS3Plugins andThen ensureHdfsPlugins
    pipe(this)
  }

  protected def copyExtRawConfigs(extRawConfigs: Map[String, String]): SubType
  protected def copyBuiltInPlugins(builtInPlugins: Set[String]): SubType

  def logTags: Map[String, String] = Map("clusterId" -> clusterId, "namespace" -> namespace, "flinkVer" -> flinkVer.fullVer)
}

object FlinkClusterDefinition {

  /**
   * Flink raw configuration keys that are not allowed to be customized by users.
   */
  lazy val notAllowCustomRawConfKeys: Vector[String] = Vector(
    "execution.target",
    "kubernetes.cluster-id",
    "kubernetes.namespace",
    "kubernetes.container.image",
    "kubernetes.service-account",
    "kubernetes.jobmanager.service-account",
    "kubernetes.pod-template-file",
    "kubernetes.pod-template-file.taskmanager",
    "kubernetes.pod-template-file.jobmanager",
    "$internal.deployment.config-dir",
    "pipeline.jars",
    "$internal.application.main",
    "$internal.application.program-args",
  )
}

object RestExportType extends ComplexEnum {
  type RestExportType = Value
  val ClusterIP         = Value("ClusterIP")
  val NodePort          = Value("NodePort")
  val LoadBalancer      = Value("LoadBalancer")
  val HeadlessClusterIP = Value("Headless_ClusterIP")
}
