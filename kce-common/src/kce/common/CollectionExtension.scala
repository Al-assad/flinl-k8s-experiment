package kce.common

import scala.util.Random

object CollectionExtension {

  private val rand = new Random()

  implicit class IterableWrapper[T](collection: Iterable[T]) {

    /**
     * Get random element from Collection.
     */
    def randomEle: Option[T] = {
      if (collection.isEmpty) None
      else if (collection.size == 1) Some(collection.head)
      else Some(collection.toVector(rand.nextInt(collection.size)))
    }
  }

  implicit class StringIterableWrapper(collection: Iterable[String]) {

    /**
     * Filter not blank String element and trim it.
     */
    def filterNotBlank(): Iterable[String] = collection
      .map(Option(_).map(_.trim).flatMap(s => if (s.isEmpty) None else Some(s)))
      .filter(_.isDefined)
      .map(_.get)
  }

}
