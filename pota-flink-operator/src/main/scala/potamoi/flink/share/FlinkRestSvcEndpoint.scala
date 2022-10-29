package potamoi.flink.share

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

}
