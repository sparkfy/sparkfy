package com.github.sparkfy.recovery

/**
 * A LeaderElectionAgent tracks current master and is a common interface for all election Agents.
 */
trait LeaderElectionAgent {
  val masterInstance: LeaderElectable

  def stop() {} // to avoid noops in implementations.
}

trait LeaderElectable {

  def electedLeader()

  def revokedLeadership()
}

/** Single-node implementation of LeaderElectionAgent -- we're initially and always the leader. */
class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
  extends LeaderElectionAgent {
  masterInstance.electedLeader()
}