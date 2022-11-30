package potamoi.flink.observer

import com.coralogix.zio.k8s.model.apps.v1.{Deployment, DeploymentCondition}
import com.coralogix.zio.k8s.model.core.v1.{Service, ServicePort}
import potamoi.flink.share.FlinkOprErr.{IllegalK8sDeploymentEntity, IllegalK8sServiceEntity}
import potamoi.flink.share.model.{FK8sDeploymentSnap, FK8sServiceSnap, SvcPort}
import zio.ZIO.{fail, succeed}
import zio.prelude.data.Optional
import zio.prelude.data.Optional.{Absent, Present}
import zio.{IO, ZIO}
import potamoi.k8s._

/**
 * Convert raw kubernetes entity to flink k8s ref entity.
 */
object K8sEntityConverter {

  implicit private class ConvertIOWrapper[E, A](io: IO[Throwable, Optional[A]]) {
    def dry(sideEf: => E): IO[E, A] = io.orElseFail(sideEf).flatMap {
      case Present(a) => succeed(a)
      case Absent     => fail(sideEf)
    }
  }

  /**
   * Convert [[Service]] to [[FK8sServiceSnap]].
   */
  def toServiceSnap(svc: Service): IO[IllegalK8sServiceEntity.type, FK8sServiceSnap] = ZIO
    .attempt {
      for {
        metadata        <- svc.metadata
        namespace       <- metadata.namespace
        name            <- metadata.name
        createTimestamp <- metadata.creationTimestamp.map(_.value.toEpochSecond)
        spec            <- svc.spec
        selector        <- spec.selector
        app             <- selector.get("app").toOptional
        component       <- selector.get("component").toOptional
        clusterIP = spec.clusterIP.toOption
        ports = spec.ports
          .getOrElse(Vector.empty[ServicePort])
          .map { svcPort =>
            for {
              portName <- svcPort.name
              protocol <- svcPort.protocol
              targetPort <- svcPort.targetPort.map(_.value match {
                case Left(v)  => v
                case Right(v) => v.toInt
              })
              port = svcPort.port
            } yield SvcPort(
              name = portName,
              protocol = protocol,
              port = port,
              targetPort = targetPort
            )
          }
          .filter(_.isDefined)
          .map(_.toOption.get)
          .toSet
      } yield FK8sServiceSnap(
        clusterId = app,
        namespace = namespace,
        name = name,
        component = component,
        clusterIP = clusterIP,
        ports = ports,
        createTime = createTimestamp
      )
    }
    .dry(IllegalK8sServiceEntity)

  /**
   * Convert [[Deployment]] to [[FK8sDeploymentSnap]].
   */
  def toDeploymentSnap(deploy: Deployment): IO[IllegalK8sDeploymentEntity.type, FK8sDeploymentSnap] = ZIO
    .attempt {
      for {
        metadata        <- deploy.metadata
        namespace       <- metadata.namespace
        name            <- metadata.name
        createTimestamp <- metadata.creationTimestamp.map(_.value.toEpochSecond)
        spec            <- deploy.spec
        selector        <- spec.selector.matchLabels
        app             <- selector.get("app").toOptional
        component       <- selector.get("component").toOptional
        status          <- deploy.status
        observedGeneration  = status.observedGeneration.getOrElse(0L)
        replicas            = status.replicas.getOrElse(0)
        readyReplicas       = status.readyReplicas.getOrElse(0)
        unavailableReplicas = status.unavailableReplicas.getOrElse(0)
        availableReplicas   = status.availableReplicas.getOrElse(0)
        updatedReplicas     = status.updatedReplicas.getOrElse(0)
        conditions = status.conditions
          .getOrElse(Vector.empty[DeploymentCondition])
          .map { cond =>
            WorkloadCondition(
              condType = cond.`type`,
              status = WorkloadCondStatus.withNameOps(cond.status).getOrElse(WorkloadCondStatus.Unknown),
              reason = cond.reason.toOption,
              message = cond.message.toOption,
              lastTransitionTime = cond.lastTransitionTime.map(_.value.toEpochSecond).toOption
            )
          }
          .sorted
          .reverse
      } yield FK8sDeploymentSnap(
        clusterId = app,
        namespace = namespace,
        name = name,
        component = component,
        observedGeneration = observedGeneration,
        conditions = conditions,
        replicas = replicas,
        readyReplicas = readyReplicas,
        unavailableReplicas = unavailableReplicas,
        availableReplicas = availableReplicas,
        updatedReplicas = updatedReplicas,
        createTime = createTimestamp
      )
    }
    .dry(IllegalK8sDeploymentEntity)

}
