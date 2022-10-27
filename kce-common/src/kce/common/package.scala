package kce

import scala.language.implicitConversions

package object common {

  val ziox = ZIOExtension

  /**
   * Contra control value to the function.
   */
  implicit class GenericPF[T](value: T) {
    @inline def contra[A](func: T => A): A = func(value)
  }

  /**
   * Trim String value safely.
   */
  def safeTrim(value: String): String = Option(value).map(_.trim).getOrElse("")

  /**
   * Auto convert value to Some
   */
  implicit def valueToSome[T](value: T): Option[T] = Some(value)

}
