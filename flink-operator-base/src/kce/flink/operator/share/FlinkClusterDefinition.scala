package kce.flink.operator.share

import cats.Eval
import kce.common.CollectionExtension.StringIterableWrapper
import kce.common.PathTool.{isS3Path, reviseToS3pSchema}
import kce.common.{safeTrim, ComplexEnum}
import kce.conf.PotaConf
import kce.flink.operator.FlinkConfigExtension.{configurationToPF, ConfigurationPF, EmptyConfiguration}
import kce.flink.operator.FlinkPlugins
import kce.flink.operator.FlinkPlugins.{s3Hadoop, s3Presto, s3aPlugins}
import kce.flink.operator.share.FlinkClusterDefinition.notAllowCustomRawConfKeys
import kce.flink.operator.share.FlinkExecMode.FlinkExecMode
import kce.flink.operator.share.RestExportType.RestExportType
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
    lazy val checkJmHa       = jmHa.exists(c => isS3Path(c.storageDir))
    lazy val checkInjectDeps = injectedDeps.exists(isS3Path)

    if (checkStateBackend) true
    else if (checkJmHa) true
    else if (checkInjectDeps) true
    else moreChecks.value
  }

  protected type RevisePipe = SubType => SubType

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def revise(): SubType

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  protected def reviseDefinition(moreRevisePipe: RevisePipe = identity): SubType = {
    // filter not allow customized extRawConfigs.
    val removeNotAllowCustomRawConfigs: RevisePipe = _.copyExtRawConfigs(
      extRawConfigs
        .map(kv => safeTrim(kv._1) -> safeTrim(kv._2))
        .filter(kv => kv._1.nonEmpty && kv._2.nonEmpty)
        .filter(kv => !notAllowCustomRawConfKeys.contains(kv._1))
    )
    // format BuiltInPlugins fields.
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
    // modify s3 path to s3p schema
    val reviseS3Path: RevisePipe = { definition =>
      definition
        .copyStateBackend(
          stateBackend.map(conf =>
            conf.copy(
              checkpointDir = conf.checkpointDir.map(reviseToS3pSchema),
              savepointDir = conf.savepointDir.map(reviseToS3pSchema)
            ))
        )
        .copyJmHa(
          jmHa.map(conf =>
            conf.copy(
              storageDir = reviseToS3pSchema(conf.storageDir)
            ))
        )
        .copyInjectDeps(
          injectedDeps.map(reviseToS3pSchema)
        )
    }
    // ensure s3 plugins is enabled if necessary.
    val ensureS3Plugins: RevisePipe = { definition =>
      val extBuildInS3PluginJar = {
        val s3pJar = s3Presto.jarName(flinkVer)
        if (isS3Required && !builtInPlugins.contains(s3pJar)) s3pJar else ""
      }
      val extJobS3PluginJar = {
        if (s3.isEmpty) ""
        else {
          val s3aJars = s3aPlugins.map(_.jarName(flinkVer))
          if ((builtInPlugins & s3aJars).isEmpty) s3Hadoop.jarName(flinkVer) else ""
        }
      }
      val extraPluginJars = Vector(extBuildInS3PluginJar, extJobS3PluginJar).filter(_.nonEmpty)
      if (extraPluginJars.nonEmpty) definition.copyBuiltInPlugins(builtInPlugins ++ extraPluginJars) else definition
    }
    // ensure hadoop plugins is enabled if necessary.
    val ensureHdfsPlugins: RevisePipe = identity

    val pipe = removeNotAllowCustomRawConfigs andThen
      completeBuiltInPlugins andThen
      reviseS3Path andThen
      ensureS3Plugins andThen
      ensureHdfsPlugins andThen
      moreRevisePipe
    pipe(this)
  }

  /**
   * Convert to Flink raw configuration.
   */
  protected def convertToFlinkConfig(kceConf: PotaConf, moreInject: ConfigurationPF => ConfigurationPF): Configuration = {
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
        val buildInS3Conf = if (isS3Required) S3AccessConf(kceConf.s3).rawMappingS3p else Vector.empty
        val jobS3Conf     = s3.map(_.rawMappingS3a).getOrElse(Vector.empty)
        (buildInS3Conf ++ jobS3Conf)
          .foldLeft(conf)((ac, c) => ac.append(c._1, c._2))
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

  protected def copyExtRawConfigs(extRawConfigs: Map[String, String]): SubType
  protected def copyBuiltInPlugins(builtInPlugins: Set[String]): SubType
  protected def copyStateBackend(stateBackend: Option[StateBackendConf]): SubType
  protected def copyJmHa(jmHa: Option[JmHaConf]): SubType
  protected def copyInjectDeps(injectedDeps: Set[String]): SubType

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
