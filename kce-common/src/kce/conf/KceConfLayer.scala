package kce.conf

import com.coralogix.zio.k8s.client.apps.v1.deployments.Deployments
import com.coralogix.zio.k8s.client.config.httpclient
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.coralogix.zio.k8s.client.v1.services.Services
import zio.{ULayer, ZIO, ZLayer}

/**
 * KceConfig ZIO layer
 */
object KceConfLayer {
  def live: ULayer[KceConf] = ZLayer(ZIO.succeed(KceConf.default))
}

/**
 * ZIO-K8s client Layer
 */
object ZIOK8sLayer {
  val live = httpclient.k8sDefault >>> (Services.live ++ Pods.live ++ Deployments.live)
}
