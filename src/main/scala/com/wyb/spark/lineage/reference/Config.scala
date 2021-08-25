package com.wyb.spark.lineage.reference

import java.util.Properties

import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._

object Config extends Logging{

  private final val prefix = "com.wyb.spark.lineage.reference"
  private lazy val properties = {
    Option(getClass.getResourceAsStream("/lineage.properties"))
      .map({ stream =>
        val props = new Properties()
        props.load(stream)
        stream.close()

        props
      })
      .getOrElse(new Properties())
  }

  def get(name: String): String = properties.getProperty(name)

  def getList(name: String): Seq[String] = Option.apply(properties.getProperty(name))
    .flatMap(p => if (p.isEmpty) None else Some(p))
    .map(p => p.split("\\s*,\\s*").toSeq)
    .getOrElse(Seq())

  def createInstanceOf[T](suffix: String): T = {
    val propPrefix = s"$prefix.$suffix"
    val className = get(propPrefix)

    createInstance(className, propPrefix)
  }

  def createInstancesOf[T](suffix: String): List[T] = {
    val propPrefix = s"$prefix.$suffix"
    logInfo(s"propPrefix : $propPrefix")

    getList(s"${propPrefix}s").map(className => {
      createInstance[T](className, propPrefix)
    }).toList
  }

  private def createInstance[T](className: String, prefix: String): T = {
    try {
      def clazz = getClass.getClassLoader.loadClass(className)
      val configKey = clazz.getSimpleName.replaceFirst("Reporter$", "").toLowerCase

      val clazzPrefix = s"$prefix.$configKey"

      val props = properties
        .toMap
        .filter({ case (k, _) => k.startsWith(s"$clazzPrefix.")})
        .map({ case (k, v) => k.substring(clazzPrefix.length + 1) -> v})

      logDebug(s"Properties -> $props")

      clazz
        .getConstructor(classOf[Map[String, String]])
        .newInstance(props)
        .asInstanceOf[T]
    } catch {
      case e: Throwable => {
        logError(s"Unable to create instance of $className", e)
        throw e
      }
    }
  }
}
