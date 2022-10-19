package kce.flink

import com.coralogix.zio.k8s.client.model.K8sNamespace

import scala.language.implicitConversions

package object operator {

  implicit def stringToK8sNamespace(namespace: String): K8sNamespace = K8sNamespace(namespace)

}
