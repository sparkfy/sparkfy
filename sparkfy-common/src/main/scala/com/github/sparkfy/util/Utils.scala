package com.github.sparkfy.util

import java.io.{IOException, InputStream}
import java.util.Properties

import com.github.sparkfy.{Logging, SparkfyException}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Created by huangyu on 15/10/11.
 */
object Utils extends Logging {

  /**
   * Return a pair of host and port extracted from the `sparkUrl`.
   *
   * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
   * host and port.
   *
   * @throws SparkfyException if `sparkUrl` is invalid.
   */
  def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "com" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new SparkfyException("Invalid master URL: " + sparkUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkfyException("Invalid master URL: " + sparkUrl, e)
    }
  }


  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrClassLoader)
    // scalastyle:on classforname
  }

  /**
   * Execute a block of code that evaluates to Unit, re-throwing any non-fatal uncaught
   * exceptions as IOException.  This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   *
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    // It would be nice to find a method on Try that did this
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }

  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  def toPath(path: String): String = {
    if (path.endsWith("/")) path else path + "/"
  }

  def absolutePath(rootPath: String, path: String): String = {
    if (path.startsWith("/") || path.contains(":")) path else rootPath + path
  }

  //  def absolutePath(rootPath: String, path: String): String = {
  //    toPath(if (path.startsWith("/") || path.contains(":")) path else rootPath + path)
  //  }


  def loadConfFile(confInputStream: InputStream): Map[String, String] = {
    val setting: mutable.Map[String, String] = new mutable.HashMap[String, String]()
    val prop = new Properties()
    prop.load(confInputStream)
    for (key <- prop.stringPropertyNames().asScala) {
      setting.getOrElseUpdate(key, prop.getProperty(key))
    }
    setting.toMap
  }

  /** Joins a list of strings using the given separator. */
  private def join(sep: String, elements: Iterable[String]): String = {
    val sb: StringBuilder = new StringBuilder
    for (e <- elements) {
      if (e != null) {
        if (sb.length > 0) {
          sb.append(sep)
        }
        sb.append(e)
      }
    }
    sb.toString
  }
}
