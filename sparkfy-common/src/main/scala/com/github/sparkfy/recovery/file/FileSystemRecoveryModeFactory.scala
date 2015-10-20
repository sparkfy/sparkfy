package com.github.sparkfy.recovery.file

import com.github.sparkfy.Logging
import com.github.sparkfy.recovery._
import com.github.sparkfy.serializer.Serializer

/**
 * LeaderAgent in this case is a no-op. Since leader is forever leader as the actual
 * recovery is made by restoring from filesystem.
 */
class FileSystemRecoveryModeFactory(conf: Map[String, String], serializer: Serializer)
  extends RecoveryFactory(conf, serializer) with Logging{

  val RECOVERY_DIR = conf.getOrElse("recovery.file.directory", "")

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   *
   */
  override def createPersistenceEngine(): PersistenceEngine = {
    logInfo("Persisting recovery state to directory: " + RECOVERY_DIR)
    new FileSystemPersistenceEngine(RECOVERY_DIR, serializer)
  }

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}
