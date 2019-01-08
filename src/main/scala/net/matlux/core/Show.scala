package net.matlux.core

import scala.util.Try

object Show {
  /**
    * Converts nested tree of case classes to a string.
    * Can be used for showing configuration.
    */
  def apply[T](p: T, indentLevel: Int = 0): String = {
    val indentChar = "  "
    val indent = indentChar * indentLevel
    p match {
      case m: Map[_, _] => m.map(x => x._1 + " : " + apply(x._2, indentLevel + 1)).mkString("{", ",\n" + (indentChar * (indentLevel + 1)), "}")
      case t: Traversable[_] => t.map(x => apply(x, indentLevel + 1)).mkString("[", ",\n" + (indentChar * (indentLevel + 1)), "]")
      case Some(x) => apply(x, indentLevel)
      case None => "None"
      case p: Product =>
        val clazz = p.getClass
        "{\n" +
          clazz.getDeclaredFields
            .filter(!_.isSynthetic).flatMap(f => Try(clazz.getMethod(f.getName)).toOption.flatMap(Option(_)).map((f.getName, _)))
            .map { case (name, getter) => indent + indentChar + name + " : " + apply(getter.invoke(p), indentLevel + 1) }
            .mkString(s",\n") +
          "\n" + indent + "}"
      case o => s"$o"
    }
  }
}

