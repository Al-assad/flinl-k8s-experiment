package kce.flink.operator.entity

/**
 * Flink K8s services rest endpoint.
 */
case class FlinkRestSvcEndpoint(svcName: String, svcNs: String, port: Int, clusterIp: String) {
  lazy val dns           = s"$svcName.$svcNs"
  lazy val dnsRest       = s"http://$dns:$port"
  lazy val clusterIpRest = s"http://$clusterIp:$port"
}
