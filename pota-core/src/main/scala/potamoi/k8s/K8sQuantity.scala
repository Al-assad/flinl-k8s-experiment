package potamoi.k8s

import potamoi.common.ComplexEnum
import potamoi.k8s.QuantityUnit.QuantityUnit

import scala.math.pow

/**
 * Kubernetes resource quantity.
 * see: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity
 */
object QuantityUnit extends ComplexEnum {
  type QuantityUnit = Value
  val n, u, m, k, M, G, T, P, E, Ki, Mi, Gi, Ti, Pi, Ei = Value

  def resolve(quantity: String): K8sQuantity = {
    val unit  = values.find(unit => quantity.endsWith(unit.toString)).getOrElse(k)
    val value = quantity.split(unit.toString)(0).toDouble
    K8sQuantity(value, unit)
  }
}

case class K8sQuantity(value: Double, unit: QuantityUnit) {
  import QuantityUnit._

  def to(targetUnit: QuantityUnit): Double = {
    if (unit == targetUnit) value
    else if (unit <= E && targetUnit <= E) value * pow(1000, unit.id - targetUnit.id)
    else if (unit >= Ki && targetUnit >= Ki) value * pow(1024, unit.id - targetUnit.id)
    else if (unit <= E) value * 1.024 * pow(1000, (targetUnit.id - Ki.id) - (unit.id - k.id))
    else value / 1.024 * pow(1000, (unit.id - Ki.id) - (targetUnit.id - k.id))
  }
}
