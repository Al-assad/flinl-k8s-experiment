package potamoi.flink.share.model

import potamoi.cluster.CborSerializable
import potamoi.config.{FlinkConf, FlkRestEndpointType}

/**
 * K8s svc endpoint of flink rest-service.
 */
case class FlinkRestSvcEndpoint(svcName: String, svcNs: String, port: Int, clusterIp: String) extends CborSerializable {

  /**
   * DNS name of the k8s svc.
   */
  lazy val dns = s"$svcName.$svcNs"

  /**
   * DNS url of the k8s svc.
   */
  lazy val dnsRest = s"http://$dns:$port"

  /**
   * Cluster-IP url of the k8s svc.
   */
  lazy val clusterIpRest = s"http://$clusterIp:$port"

  /**
   * Choose rest url type by [[FlinkConf]].
   */
  def chooseUrl(implicit flinkConf: FlinkConf): String = flinkConf.restEndpointTypeInternal match {
    case FlkRestEndpointType.SvcDns    => dnsRest
    case FlkRestEndpointType.ClusterIp => clusterIpRest
  }

  /**
   * Choose host type by [[FlinkConf]].
   */
  def chooseHost(implicit flinkConf: FlinkConf): String = flinkConf.restEndpointTypeInternal match {
    case FlkRestEndpointType.SvcDns    => dns
    case FlkRestEndpointType.ClusterIp => clusterIp
  }

}

object FlinkRestSvcEndpoint {

  def of(svcSnap: FK8sServiceSnap): Option[FlinkRestSvcEndpoint] =
    if (!svcSnap.isFlinkRestSvc) None
    else {
      for {
        clusterIp <- svcSnap.clusterIP
        port      <- svcSnap.ports.find(_.name == "rest").map(_.port)
        name = svcSnap.name
        ns   = svcSnap.namespace
      } yield FlinkRestSvcEndpoint(name, ns, port, clusterIp)
    }
}
