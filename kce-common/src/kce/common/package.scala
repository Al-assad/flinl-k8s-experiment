package kce

import scala.language.implicitConversions

package object common {

  /**
   * Contra control value to the function.
   */
  implicit class GenericPF[T](value: T) {
    @inline def contra[A](func: T => A): A = func(value)
  }

  val os = OsTool

  /**
   * Trim String value safely.
   */
  def safeTrim(value: String): String = Option(value).map(_.trim).getOrElse("")

}