package kce.flink.operator

import com.coralogix.zio.k8s.model.core.v1.Pod
import io.circe.syntax._

/**
 * Flink K8s PodTemplate resolver.
 */
object PodTemplateResolver {

  def resolvePodTemplate(): Pod = { ??? }

}

object test extends App {
//
//  val template = Pod(
//    metadata = ObjectMeta(
//      name = "flink-pod-template"
//    ),
//    spec = PodSpec(
//      initContainers = Vector(
//        Container(
//          name = "usrlib-loader",
//          image = "minio/mc",
//          command = Vector("sh", "-c", "mc alias set minio http://192.168.3.17:30255 minio minio123"),
//          volumeMounts = Vector(
//            VolumeMount(
//              name = "flink-usrlib",
//              mountPath = "/opt/flink/usrlib"
//            )
//          )
//        )
//      )
//    )
//  )
//
//  println(template.asJson.deepDropNullValues.asYaml.spaces2)

//  println(template.asJson.asYaml.spaces2)

}
