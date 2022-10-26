package kce.flink.operator

import com.coralogix.zio.k8s.model.core.v1._
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import io.circe.syntax._
import io.circe.yaml.parser.{parse => parseYaml}
import io.circe.yaml.syntax._
import kce.common.LogMessageTool.LogMessageStringWrapper
import kce.common.PathTool.{isS3Path, purePath}
import kce.conf.KceConf
import kce.flink.operator.entity.{FlinkAppClusterDef, FlinkClusterDefinition}
import kce.fs.lfs
import zio.prelude.data.Optional.{Absent, Present}
import zio.{IO, ZIO}

/**
 * Flink K8s PodTemplate resolver.
 */
object PodTemplateResolver {

  /**
   * Generate PodTemplate and dump it to local dir, return the yaml file path on local fs.
   */
  def resolvePodTemplateAndDump(definition: FlinkClusterDefinition[_], kceConf: KceConf): IO[PodTemplateResolveErr, String] = {
    for {
      podTemplate     <- resolvePodTemplate(definition, kceConf)
      podTemplatePath <- ZIO.succeed(s"${kceConf.flink.localTmpDir}/${definition.namespace}@${definition.clusterId}/flink-podtemplate.yaml")
      _               <- writeToLocal(podTemplate, podTemplatePath)
    } yield podTemplatePath
  }

  /**
   * Resolve and generate PodTemplate from Flink cluster definition.
   */
  def resolvePodTemplate(definition: FlinkClusterDefinition[_], kceConf: KceConf): IO[PodTemplateResolveErr, Pod] = {
    definition.overridePodTemplate match {
      case Some(podTemplate) =>
        ZIO
          .fromEither(parseYaml(podTemplate).map(_.as[Pod]).flatMap(identity))
          .mapError(PodTemplateResolveErr("Unable to parse the override podtemplate yaml content.", _))
      case None =>
        ZIO
          .attempt(genPodTemplate(definition, kceConf))
          .mapError(PodTemplateResolveErr("Fail to generate podtemplate.", _))
    }
  }

  /**
   * Generate PodTemplate from Flink cluster definition.
   */
  private def genPodTemplate(definition: FlinkClusterDefinition[_], kceConf: KceConf): Pod = {
    // user libs on s3: (pure path, jar name)
    val splitPath: String => (String, String) = path => purePath(path) -> path.split('/').last
    val libsOnS3 = {
      val thirdPartyLibs = definition.injectedDeps.filter(isS3Path).map(splitPath)
      val jobJarLib = definition match {
        case app: FlinkAppClusterDef => Option(app.jobJar).filter(isS3Path).map(splitPath)
        case _                       => None
      }
      thirdPartyLibs ++ jobJarLib
    }

    // userlib-loader initContainer
    lazy val cpS3LibClauses = libsOnS3
      .map { case (path, name) => kceConf.s3.revisePath(path) -> name }
      .map { case (path, name) => s"&& mc cp minio/${path} /opt/flink/lib/$name" }
      .mkString(" ")

    val libLoaderInitContainer =
      if (libsOnS3.isEmpty) None
      else
        Some(
          Container(
            name = "userlib-loader",
            image = kceConf.flink.minioClientImage,
            command = Vector("sh", "-c", s"mc alias set minio ${kceConf.s3.endpoint} ${kceConf.s3.accessKey} ${kceConf.s3.secretKey} $cpS3LibClauses"),
            volumeMounts = Vector(VolumeMount(name = "flink-libs", mountPath = "/opt/flink/lib")))
        )
    val initContainers = Vector(libLoaderInitContainer).flatten

    // pod template definition
    val podDef = Pod(
      metadata = ObjectMeta(name = "pod-template"),
      spec = PodSpec(
        volumes = Vector(
          Volume(name = "flink-volume-hostpath", hostPath = HostPathVolumeSource(path = "/tmp", `type` = "Directory")),
          Volume(name = "flink-libs", emptyDir = EmptyDirVolumeSource()),
          Volume(name = "flink-logs", emptyDir = EmptyDirVolumeSource())
        ),
        initContainers = if (initContainers.isEmpty) Absent else Present(initContainers),
        containers = Vector(
          Container(
            name = "flink-main-container",
            volumeMounts = Vector(
              VolumeMount(name = "flink-volume-hostpath", mountPath = "/opt/flink/volume"),
              VolumeMount(name = "flink-logs", mountPath = "/opt/flink/log"),
            ) ++ libsOnS3.map { case (_, name) =>
              VolumeMount(name = "flink-libs", mountPath = s"/opt/flink/lib/$name", subPath = name)
            })
        ))
    )
    podDef
  }

  /**
   * Write the Pod to a local temporary file in yaml format.
   * Return generated yaml file path.
   */
  def writeToLocal(podTemplate: Pod, path: String): IO[PodTemplateResolveErr, Unit] = {
    for {
      yaml <- ZIO.succeed(podTemplate.asJson.deepDropNullValues.asYaml.spaces2)
      _    <- lfs.rm(path)
      _    <- lfs.write(path, yaml)
    } yield ()
  }.mapError(PodTemplateResolveErr(s"Fail to write podtemplate to local file." <> "path" -> path, _))
}
