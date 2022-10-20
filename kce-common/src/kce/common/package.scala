package kce

package object common {

  /**
   * Contra control value to the function.
   */
  implicit class GenericPF[T](value: T) {
    @inline def contraPF[A](func: T => A): A = func(value)
  }

  val os = OsTool

}
