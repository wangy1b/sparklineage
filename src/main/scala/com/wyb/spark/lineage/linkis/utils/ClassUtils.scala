package com.wyb.spark.lineage.linkis.utils


import java.lang.reflect.Modifier


object ClassUtils {

  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf("!")))
      } else {
        None
      }
    } else {
      None
    }
  }

  def getClassInstance[T](className: String): T = {
    Thread.currentThread.getContextClassLoader.loadClass(className).asInstanceOf[Class[T]].newInstance()
  }

  def getFieldVal(o: Any, name: String): Any = {
    try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } catch {
      case t: Throwable => throw t
    }
  }

  def setFieldVal(o: Any, name: String, value: Any): Unit = {
    try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.set(o, value.asInstanceOf[AnyRef])
    } catch {
      case t: Throwable => throw t
    }
  }

  def isInterfaceOrAbstract(clazz: Class[_]): Boolean = {
    clazz.isInterface || Modifier.isAbstract(clazz.getModifiers)
  }


}
