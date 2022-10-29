package potamoi

import org.scalatest.Tag

package object testkit {

  /**
   * Test suit only enabled for local dev environment.
   */
  object UnsafeEnv extends Tag("unsafeEnv")

}
