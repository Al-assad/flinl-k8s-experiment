package potamoi.k8s

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes pod metrics.
 */
case class PodMetrics(timestamp: Long, containers: Vector[ContainerMetrics] = Vector.empty)

/**
 * cpu unit: m,  memory unit: Ki
 */
case class ContainerMetrics(name: String, cpu: K8sQuantity, memory: K8sQuantity)

object PodMetrics {
  implicit val containerMetricsCodec: JsonCodec[ContainerMetrics] = DeriveJsonCodec.gen[ContainerMetrics]
  implicit val podMetricsCodec: JsonCodec[PodMetrics]             = DeriveJsonCodec.gen[PodMetrics]
}
