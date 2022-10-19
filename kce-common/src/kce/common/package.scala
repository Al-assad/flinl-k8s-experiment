package kce

package object common {

  /**
   * Contra control value to the function.
   */
  implicit class GenericPF[T](value: T) {
    def contraPF[A](func: T => A): A = func(value)
  }

}
