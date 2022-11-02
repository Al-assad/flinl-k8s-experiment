package potamoi

import scala.language.implicitConversions

package object common {

  val ziox  = ZIOExtension
  val paths = PathTool

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

  /**
   * A more reader-friendly version of toString.
   */
  def toPrettyString(value: Any): String = pprint.apply(value).render

  implicit class PrettyPrintable(value: AnyRef) {
    def toPrettyString: String = common.toPrettyString(value)
  }

}
