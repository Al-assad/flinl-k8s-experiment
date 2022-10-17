package kce.common

import scala.util.Random

object CollectionExtension {

  private val rand = new Random()

  implicit class IterableWrapper[T](collection: Iterable[T]) {
    def randomEle: Option[T] = {
      if (collection.isEmpty) None
      else if (collection.size == 1) Some(collection.head)
      else Some(collection.toVector(rand.nextInt(collection.size)))
    }
  }
}
