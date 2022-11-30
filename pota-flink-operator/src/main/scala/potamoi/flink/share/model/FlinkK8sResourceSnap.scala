package potamoi.flink.share.model

import potamoi.curTs
import potamoi.k8s.WorkloadCondition
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink k8s component name that storage in k8s metadata.
 */
object FlK8sComponentName {
  val jobmanager  = "jobmanager"
  val taskmanager = "taskmanager"
}

/**
 * Flink k8s service resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toServiceSnap]]
 */
case class FK8sServiceSnap(
    clusterId: String,
    namespace: String,
    name: String,
    component: String,
    dns: String,
    clusterIP: Option[String],
    ports: Set[SvcPort],
    isFlinkRestSvc: Boolean,
    createTime: Long,
    ts: Long = curTs)

case class SvcPort(name: String, protocol: String, port: Int, targetPort: Int)

object FK8sServiceSnap {
  def apply(
      clusterId: String,
      namespace: String,
      name: String,
      component: String,
      clusterIP: Option[String],
      ports: Set[SvcPort],
      createTime: Long): FK8sServiceSnap =
    FK8sServiceSnap(
      clusterId = clusterId,
      namespace = namespace,
      name = name,
      component = component,
      clusterIP = clusterIP,
      ports = ports,
      createTime = createTime,
      dns = s"$name.$namespace",
      isFlinkRestSvc = name.endsWith("-rest"),
      ts = curTs
    )

  implicit val svcPortCodec: JsonCodec[SvcPort]  = DeriveJsonCodec.gen[SvcPort]
  implicit val codec: JsonCodec[FK8sServiceSnap] = DeriveJsonCodec.gen[FK8sServiceSnap]
}

/**
 * Flink k8s deployment resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toDeploymentSnap]]
 */
case class FK8sDeploymentSnap(
    clusterId: String,
    namespace: String,
    name: String,
    observedGeneration: Long,
    component: String,
    conditions: Vector[WorkloadCondition],
    replicas: Int,
    readyReplicas: Int,
    unavailableReplicas: Int,
    availableReplicas: Int,
    updatedReplicas: Int,
    createTime: Long,
    ts: Long = curTs)

object FK8sDeploymentSnap {
  implicit val codec: JsonCodec[FK8sDeploymentSnap] = DeriveJsonCodec.gen[FK8sDeploymentSnap]
}

/**
 * Flink k8s pod resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toPodSnap]]
 */
case class FK8sPodSnap(
    clusterId: String,
    namespace: String,
    name: String,
    createTime: Long,
    startTime: Long)
