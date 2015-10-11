package com.github.sparkfy.util

import com.github.sparkfy.rpc.RpcTimeout


/**
 * Created by huangyu on 15/10/11.
 */
object RpcUtils {

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: Map[String, String]): RpcTimeout = {
    RpcTimeout(conf, Seq("rpc.lookupTimeout"), "120s")
  }
}
