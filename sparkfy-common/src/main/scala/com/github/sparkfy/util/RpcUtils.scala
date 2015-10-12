package com.github.sparkfy.util

import com.github.sparkfy.rpc.RpcTimeout
import com.github.sparkfy.util.MapWrapper._

/**
 * Created by huangyu on 15/10/11.
 */
object RpcUtils {

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: Map[String, String]): Int = {
    conf.getInt("rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: Map[String, String]): Long = {
    Utils.timeStringAsMs(conf.get("rpc.retry.wait", "3s"))
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: Map[String, String]): RpcTimeout = {
    RpcTimeout(conf, Seq("rpc.lookupTimeout", "network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  private[spark] def askRpcTimeout(conf: Map[String, String]): RpcTimeout = {
    RpcTimeout(conf, Seq("rpc.askTimeout", "network.timeout"), "120s")
  }

}
