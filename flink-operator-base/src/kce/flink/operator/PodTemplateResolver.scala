package kce.flink.operator

import com.coralogix.zio.k8s.model.core.v1._
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import io.circe.Error
import io.circe.syntax._
import io.circe.yaml.parser.{parse => parseYaml}
import io.circe.yaml.syntax._
import kce.common.PathTool.purePath
import kce.common.S3Tool.isS3Path
import kce.common.os
import kce.conf.KceConf
import kce.flink.operator.entity.FlinkSessClusterDef
import zio.{IO, ZIO}
import zio.prelude.data.Optional.{Absent, Present}

/**
 * Flink K8s PodTemplate resolver.
 */
object PodTemplateResolver {

  /**
   * Resolve and generate PodTemplate from Flink cluster definition.
   */
  def resolvePodTemplate(definition: FlinkSessClusterDef): ZIO[KceConf, Error, Pod] = {
    for {
      conf <- ZIO.service[KceConf]
      rs <- definition.overridePodTemplate match {
        case Some(podTemplate) => ZIO.fromEither(parseYaml(podTemplate).map(_.as[Pod]).flatMap(identity))
        case None              => ZIO.succeed(genPodTemplate(definition, conf))
      }
    } yield rs
  }

  /**
   * Generate PodTemplate from Flink cluster definition.
   */
  private def genPodTemplate(definition: FlinkSessClusterDef, kceConf: KceConf): Pod = {
    // user libs
    val libs = definition.injectedDeps
      .filter(isS3Path)
      .map(dep => purePath(dep) -> dep.split('/').last)
    lazy val cpLibClauses = libs.map { case (path, name) => s"&& mc cp minio/$path /opt/flink/lib/$name " }

    // userlib-loader initContainer
    val libLoaderInitContainer =
      if (libs.isEmpty) None
      else
        Some(
          Container(
            name = "userlib-loader",
            image = kceConf.flink.minioClientImage,
            command = Vector(
              "sh",
              "-c",
              s"""mc alias set minio ${kceConf.s3.endpoint} ${kceConf.s3.accessKey} ${kceConf.s3.secretKey}
                 |$cpLibClauses""".stripMargin),
            volumeMounts = Vector(
              VolumeMount(name = "flink-libs", mountPath = "/opt/flink/lib"),
              VolumeMount(name = "flink-libs", mountPath = "/opt/flink/usrlib")
            ))
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
              VolumeMount(name = "flink-libs", mountPath = "/opt/flink/usrlib")
            ) ++ libs.map { case (_, name) =>
              VolumeMount(name = "flink-libs", mountPath = s"/opt/flink/lib/$name", subPath = name)
            })
        ))
    )
    podDef
  }

  /**
   * Write the Pod to a local temporary file in yaml format.
   * @return yaml file path
   */
  def writeToLocal(podTemplate: Pod, path: String): IO[Throwable, Unit] = {
    for {
      yaml <- ZIO.succeed(podTemplate.asJson.deepDropNullValues.asYaml.spaces2)
      _    <- os.rm(path)
      _    <- os.write(path, yaml)
    } yield ()
  }

}
