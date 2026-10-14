package org.apache.spark

/** The listener bus is private[spark]; benchmarks need to wait for task-end events to arrive. */
object ListenerBusAccess {
  def drain(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty()
}
