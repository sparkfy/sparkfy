package com.github.sparkfy

import com.github.sparkfy.util.Log4jPropertyHelper

/**
 * Created by yellowhuang on 2015/10/13.
 */
object LoggingTest extends Logging {

  def main(args: Array[String]) = {
    logInfo("logging test")
    Log4jPropertyHelper.updateLog4jConfiguration("conf/log4j.properties")
    logInfo("logging test")
    logError("error")
  }
}
