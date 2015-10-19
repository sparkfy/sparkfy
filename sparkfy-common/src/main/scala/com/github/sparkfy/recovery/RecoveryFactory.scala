package com.github.sparkfy.recovery

import com.github.sparkfy.serializer.Serializer

/**
 * Implementation of this class can be plugged in as recovery mode alternative.
 */
abstract class RecoveryFactory(conf: Map[String, String], serializer: Serializer) {

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   *
   */
  def createPersistenceEngine(): PersistenceEngine

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent

}
