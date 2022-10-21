package kce.common

/**
 * Extension for Number.
 */
object NumExtension {

  implicit class IntWrapper(value: Int) {
    @inline def ensureMin(min: Int): Int                          = if (value >= min) value else min
    @inline def ensureOr(cond: Int => Boolean, orValue: Int): Int = if (cond(value)) value else orValue
  }

  implicit class DoubleWrapper(value: Double) {
    @inline def ensureMin(min: Double): Double                             = if (value >= min) value else min
    @inline def ensureOr(cond: Double => Boolean, orValue: Double): Double = if (cond(value)) value else orValue
  }

}
