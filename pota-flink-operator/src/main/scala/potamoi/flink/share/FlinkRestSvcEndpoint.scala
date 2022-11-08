package potamoi.flink.share

import potamoi.conf.{FlinkConf, FlkRestEndpointType}

/**
 * K8s svc endpoint of flink rest-service.
 */
case class FlinkRestSvcEndpoint(svcName: String, svcNs: String, port: Int, clusterIp: String) {

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

}
