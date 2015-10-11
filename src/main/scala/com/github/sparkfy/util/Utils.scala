package com.github.sparkfy.util

import com.github.sparkfy.SparkfyException

/**
 * Created by huangyu on 15/10/11.
 */
object Utils {

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
      if (uri.getScheme != "sparkfy" ||
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

}
