package io.prophecy

package object scalagems {
  import scala.annotation.{StaticAnnotation, compileTimeOnly}
  import scala.language.experimental.macros
  import scala.reflect.macros.whitebox

  @compileTimeOnly("Enable macro paradise to expand macro annotations")
  class AddMethodToClass extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro AddMethodToClassImpl.impl
  }

  object AddMethodToClassImpl {
    def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
      import c.universe._

      val result = annottees.map(_.tree).toList match {
        case q"$mods class $tpname[..$tparams] $ctorMods(...$paramss) extends ..$parents { $self => ..$stats }" :: Nil =>
          val newMethod = q"""def myNewMethod(): Unit = println("New method added by smacro annotation")"""
          val updatedStats = stats :+ newMethod
          q"""
              $mods class $tpname[..$tparams] $ctorMods(...$paramss) extends ..$parents { $self =>
                ..$updatedStats
              }
            """
        case _ =>
          c.abort(c.enclosingPosition, "Annotation can only be applied to classes")
      }

      c.Expr[Any](result)
    }
  }

}
