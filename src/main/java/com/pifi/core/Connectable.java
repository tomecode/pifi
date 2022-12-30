package com.pifi.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides thread-safe access to a PRocessor as it exists within a controlled
 * flow. This node keeps track of the processor, its scheduling information and its relationships to
 * other processors and whatever scheduled futures exist for it. Must be thread safe.
 *
 */
public class Connectable {

  public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  public static final String DEFAULT_YIELD_PERIOD = "1 sec";
  public static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";

  private final AtomicInteger concurrentTaskCount;

  private final AtomicReference<List<Connection>> incomingConnections;
  private final AtomicReference<Set<Relationship>> undefinedRelationshipsToTerminate;
  private final Map<Connection, Connectable> destinations;
  private final Map<Relationship, Set<Connection>> connections;
  // private SchedulingStrategy schedulingStrategy; // guarded by synchronized keyword
  private volatile boolean hasActiveThreads = false;
  private final AtomicLong schedulingNanos;
  private volatile ScheduledState desiredState = ScheduledState.STOPPED;


  private static final Logger log = LoggerFactory.getLogger(Connectable.class);

  private final Map<Thread, ActiveTask> activeThreads = new ConcurrentHashMap<>(48);
  private final AtomicReference<Processor> processorRef;

  public static final long MINIMUM_SCHEDULING_NANOS = 1000L;

  protected final AtomicReference<ScheduledState> scheduledState;

  private final String id;

  public Connectable(Processor processor, ProcessScheduler processScheduler) {
    this.id = processor.getIdentifier();
    this.scheduledState = new AtomicReference<>(ScheduledState.STOPPED);
    schedulingNanos = new AtomicLong(MINIMUM_SCHEDULING_NANOS);

    concurrentTaskCount = new AtomicInteger(1);
    undefinedRelationshipsToTerminate = new AtomicReference<>(Collections.emptySet());
    destinations = new ConcurrentHashMap<>();
    connections = new ConcurrentHashMap<>();
    this.processorRef = new AtomicReference<>(processor);
    incomingConnections = new AtomicReference<>(new ArrayList<>());
  }

  public String getIdentifier() {
    return this.id;
  }

  public int hashCode() {
    return 273171 * id.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof Connectable)) {
      return false;
    }

    final Connectable other = (Connectable) obj;
    return id.equals(other.getIdentifier());
  }

  public Collection<Relationship> getRelationships() {
    final Processor processor = processorRef.get();
    return processor.getRelationships();
  }

  // @Override
  public ScheduledState getScheduledState() {
    // ScheduledState sc = this.scheduledState.get();
    // if (sc == ScheduledState.STARTING) {
    // final ValidationStatus validationStatus = getValidationStatus();
    //
    // if (validationStatus == ValidationStatus.INVALID) {
    // return ScheduledState.STOPPED;
    // } else {
    // return ScheduledState.RUNNING;
    // }
    // } else if (sc == ScheduledState.STOPPING) {
    // return ScheduledState.STOPPED;
    // }
    // // return sc;
    return ScheduledState.RUNNING;
  }

  /**
   * Returns the physical state of this processor which includes transition states such as STOPPING
   * and STARTING.
   *
   * @return the physical state of this processor [DISABLED, STOPPED, RUNNING, STARTING, STOPPING]
   */
  public ScheduledState getPhysicalScheduledState() {
    return this.scheduledState.get();
  }

  /**
   * @param relationshipName name
   * @return the relationship for this nodes processor for the given name or creates a new
   *         relationship for the given name
   */
  public Relationship getRelationship(final String relationshipName) {
    final Relationship specRel = new Relationship.Builder().name(relationshipName).build();
    Relationship returnRel = specRel;

    final Processor processor = processorRef.get();
    for (final Relationship rel : processor.getRelationships()) {
      if (rel.equals(specRel)) {
        returnRel = rel;
        break;
      }
    }
    return returnRel;
  }

  public void addConnection(final Connection connection) {
    Objects.requireNonNull(connection, "connection cannot be null");

    if (!connection.getSource().equals(this) && !connection.getDestination().equals(this)) {
      throw new IllegalStateException(
          "Cannot a connection to a ProcessorNode for which the ProcessorNode is neither the Source nor the Destination");
    }

    try {
      List<Connection> updatedIncoming = null;
      if (connection.getDestination().equals(this)) {
        // don't add the connection twice. This may occur if we have a
        // self-loop because we will be told
        // to add the connection once because we are the source and again
        // because we are the destination.
        final List<Connection> incomingConnections = getIncomingConnections();
        updatedIncoming = new ArrayList<>(incomingConnections);
        if (!updatedIncoming.contains(connection)) {
          updatedIncoming.add(connection);
        }
      }

      if (connection.getSource().equals(this)) {
        // don't add the connection twice. This may occur if we have a
        // self-loop because we will be told
        // to add the connection once because we are the source and again
        // because we are the destination.
        if (!destinations.containsKey(connection)) {
          for (final Relationship relationship : connection.getRelationships()) {
            final Relationship rel = getRelationship(relationship.getName());
            Set<Connection> set = connections.get(rel);
            if (set == null) {
              set = new HashSet<>();
              connections.put(rel, set);
            }

            set.add(connection);

            destinations.put(connection, connection.getDestination());
          }

          final Set<Relationship> autoTerminated = this.undefinedRelationshipsToTerminate.get();
          if (autoTerminated != null) {
            autoTerminated.removeAll(connection.getRelationships());
            this.undefinedRelationshipsToTerminate.set(autoTerminated);
          }
        }
      }

      if (updatedIncoming != null) {
        setIncomingConnections(Collections.unmodifiableList(updatedIncoming));
      }
    } finally {
      log.debug("Resetting Validation State of {} due to connection added", this);
    }
  }

  private void setIncomingConnections(final List<Connection> incoming) {
    this.incomingConnections.set(incoming);
    log.debug("Resetting Validation State of {} due to setting incoming connections", this);
  }


  public boolean hasIncomingConnection() {
    return !getIncomingConnections().isEmpty();
  }

  public void updateConnection(final Connection connection) throws IllegalStateException {
    try {
      if (java.util.Objects.requireNonNull(connection).getSource().equals(this)) {
        // update any relationships
        //
        // first check if any relations were removed.
        final List<Relationship> existingRelationships = new ArrayList<>();
        for (final Map.Entry<Relationship, Set<Connection>> entry : connections.entrySet()) {
          if (entry.getValue().contains(connection)) {
            existingRelationships.add(entry.getKey());
          }
        }

        for (final Relationship rel : connection.getRelationships()) {
          if (!existingRelationships.contains(rel)) {
            // relationship was removed. Check if this is legal.
            final Set<Connection> connectionsForRelationship = getConnections(rel);
            if (connectionsForRelationship != null && connectionsForRelationship.size() == 1 && this.isRunning() && !isAutoTerminated(rel) &&
                getRelationships().contains(rel)) {
              // if we are running and we do not terminate undefined
              // relationships and this is the only
              // connection that defines the given relationship, and
              // that relationship is required,
              // then it is not legal to remove this relationship from
              // this connection.
              throw new IllegalStateException("Cannot remove relationship " + rel.getName() +
                  " from Connection because doing so would invalidate Processor " + this + ", which is currently running");
            }
          }
        }

        // remove the connection from any list that currently contains
        for (final Set<Connection> list : connections.values()) {
          list.remove(connection);
        }

        // add the connection in for all relationships listed.
        for (final Relationship rel : connection.getRelationships()) {
          Set<Connection> set = connections.get(rel);
          if (set == null) {
            set = new HashSet<>();
            connections.put(rel, set);
          }
          set.add(connection);
        }

        // update to the new destination
        destinations.put(connection, connection.getDestination());

        final Set<Relationship> autoTerminated = this.undefinedRelationshipsToTerminate.get();
        if (autoTerminated != null) {
          autoTerminated.removeAll(connection.getRelationships());
          this.undefinedRelationshipsToTerminate.set(autoTerminated);
        }
      }

      if (connection.getDestination().equals(this)) {
        // update our incoming connections -- we can just remove & re-add
        // the connection to update the list.
        final List<Connection> incomingConnections = getIncomingConnections();
        final List<Connection> updatedIncoming = new ArrayList<>(incomingConnections);
        updatedIncoming.remove(connection);
        updatedIncoming.add(connection);
        setIncomingConnections(Collections.unmodifiableList(updatedIncoming));
      }
    } finally {
      // need to perform validation in case selected relationships were changed.
      log.debug("Resetting Validation State of {} due to updating connection", this);
    }
  }


  public List<Connection> getIncomingConnections() {
    return incomingConnections.get();
  }

  public Set<Connection> getConnections() {
    final Set<Connection> allConnections = new HashSet<>();
    for (final Set<Connection> connectionSet : connections.values()) {
      allConnections.addAll(connectionSet);
    }

    return allConnections;
  }

  @SuppressWarnings("unchecked")
  public Set<Connection> getConnections(final Relationship relationship) {
    try {
      if (relationship == null) {
        return connections.values().stream().flatMap(e -> e.stream()).collect(Collectors.toSet());// .stream().collect(Collectors.toSet());
        // return Collections.unmodifiableSet(connections.values());
      }
      final Set<Connection> applicableConnections = connections.get(relationship);
      return (applicableConnections == null) ? Collections.emptySet() : Collections.unmodifiableSet(applicableConnections);

    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return Collections.EMPTY_SET;
  }

  public String getName() {
    return null;
  }


  public boolean isAutoTerminated(final Relationship relationship) {
    final boolean markedAutoTerminate = relationship.isAutoTerminated() || undefinedRelationshipsToTerminate.get().contains(relationship);
    return markedAutoTerminate && getConnections(relationship).isEmpty();
  }

  public void onTrigger(ProcessContext context, ProcessSession session) throws Exception {
    final Processor processor = processorRef.get();
    activateThread();
    try {
      processor.onTrigger(context, session);
    } finally {
      deactivateThread();
    }
  }

  private void activateThread() {
    final Thread thread = Thread.currentThread();
    final Long timestamp = System.currentTimeMillis();
    activeThreads.put(thread, new ActiveTask(timestamp));
  }

  private void deactivateThread() {
    activeThreads.remove(Thread.currentThread());
  }

  /**
   * @return the number of tasks that may execute concurrently for this processor
   */

  public int getMaxConcurrentTasks() {
    return concurrentTaskCount.get();
  }

  public boolean isRunning() {
    return getScheduledState().equals(ScheduledState.RUNNING) || hasActiveThreads;
  }



  /**
   * Will idempotently start the processor
   */
  public void start(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis, final long processorStartTimeoutMillis,
      final SchedulingAgentCallback schedulingAgentCallback) {

    ScheduledState desiredState = ScheduledState.RUNNING;
    ScheduledState scheduledState = ScheduledState.STARTING;

    log.info("Starting {}", this);

    ScheduledState currentState;
    boolean starting;
    synchronized (this) {
      currentState = this.scheduledState.get();

      if (currentState == ScheduledState.STOPPED) {
        starting = this.scheduledState.compareAndSet(ScheduledState.STOPPED, scheduledState);
        if (starting) {
          this.desiredState = desiredState;
        }
      } else if (currentState == ScheduledState.STOPPING) {
        this.desiredState = desiredState;
        return;
      } else {
        starting = false;
      }
    }
    // will ensure that the Processor represented by this node can only be started
    // once
    if (starting) {
      initiateStart(taskScheduler, administrativeYieldMillis, processorStartTimeoutMillis, schedulingAgentCallback);
    } else {
      final String procName = processorRef.get().toString();
      log.warn("Cannot start {} because it is not currently stopped. Current state is {}", procName, currentState);
    }
  }

  public Processor getProcessor() {
    return processorRef.get();
  }

  private void initiateStart(final ScheduledExecutorService taskScheduler, final long administrativeYieldMillis,
      final long processorStartTimeoutMillis, final SchedulingAgentCallback schedulingAgentCallback) {

    final Processor processor = getProcessor();

    // Completion Timestamp is set to MAX_VALUE because we don't want to timeout
    // until the task has a chance to run.
    final AtomicLong completionTimestampRef = new AtomicLong(Long.MAX_VALUE);

    // Create a task to invoke the @OnScheduled annotation of the processor
    final Callable<Void> startupTask = () -> {
      final ScheduledState currentScheduleState = scheduledState.get();
      if (currentScheduleState == ScheduledState.STOPPING || currentScheduleState == ScheduledState.STOPPED ||
          getDesiredState() == ScheduledState.STOPPED) {
        log.debug("{} is stopped. Will not call @OnScheduled lifecycle methods or begin trigger onTrigger() method", Connectable.this);
        schedulingAgentCallback.onTaskComplete();
        scheduledState.set(ScheduledState.STOPPED);
        return null;
      }

      // log.debug("Invoking @OnScheduled methods of {}", processor);

      // Now that the task has been scheduled, set the timeout
      long scheduledTimeout = System.currentTimeMillis() + processorStartTimeoutMillis;
      log.debug("scheduled timeout={}", scheduledTimeout);
      completionTimestampRef.set(scheduledTimeout);

      try {
        hasActiveThreads = true;
        if ((desiredState == ScheduledState.RUNNING)) {
          log.debug("Successfully completed the @OnScheduled methods of {}; will now start triggering processor to run", processor);
          schedulingAgentCallback.trigger(); // callback provided by StandardProcessScheduler to
                                             // essentially initiate component's onTrigger() cycle
        } else {
          log.info(
              "Successfully invoked @OnScheduled methods of {} but scheduled state is no longer STARTING so will stop processor now; current state = {}, desired state = {}",
              processor, scheduledState.get(), desiredState);

          // can only happen if stopProcessor was called before service was transitioned
          // to RUNNING state
          activateThread();
          try {
            hasActiveThreads = false;
          } finally {
            deactivateThread();
          }

          scheduledState.set(ScheduledState.STOPPED);
        }
      } finally {
        schedulingAgentCallback.onTaskComplete();
      }

      return null;
    };

    // Trigger the task in a background thread.
    // final Future<?> taskFuture =
    schedulingAgentCallback.scheduleTask(startupTask);

    // Trigger a task periodically to check if @OnScheduled task completed. Once it
    // has,
    // this task will call SchedulingAgentCallback#onTaskComplete.
    // However, if the task times out, we need to be able to cancel the monitoring.
    // So, in order
    // to do this, we use #scheduleWithFixedDelay and then make that Future
    // available to the task
    // itself by placing it into an AtomicReference.
    // final AtomicReference<Future<?>> futureRef = new AtomicReference<>();
    // final Runnable monitoringTask = new Runnable() {
    // @Override
    // public void run() {
    // Future<?> monitoringFuture = futureRef.get();
    // if (monitoringFuture == null) { // Future is not yet available. Just return and wait for the next
    // // invocation.
    // return;
    // }
    //
    // monitorAsyncTask(taskFuture, monitoringFuture, completionTimestampRef.get());
    // }
    // };
    //
    // final Future<?> future = taskScheduler.scheduleWithFixedDelay(monitoringTask, 1, 1000,
    // TimeUnit.MILLISECONDS);
    // futureRef.set(future);
  }

  // private void monitorAsyncTask(final Future<?> taskFuture, final Future<?> monitoringFuture,
  // final long completionTimestamp) {
  // if (taskFuture.isDone()) {
  // monitoringFuture.cancel(false); // stop scheduling this task
  // } else if (System.currentTimeMillis() > completionTimestamp) {
  // // Task timed out. Request an interrupt of the processor task
  // taskFuture.cancel(true);
  //
  // // Stop monitoring the processor. We have interrupted the thread so that's all
  // // we can do. If the processor responds to the interrupt, then
  // // it will be re-scheduled. If it does not, then it will either keep the thread
  // // indefinitely or eventually finish, at which point
  // // the Processor will begin running.
  // monitoringFuture.cancel(false);
  //
  // final Processor processor = processorRef.get();// .getProcessor();
  // log.warn("Timed out while waiting for OnScheduled of " + processor +
  // " to finish. An attempt is made to cancel the task via Thread.interrupt(). However it does not "
  // +
  // "guarantee that the task will be canceled since the code inside current OnScheduled operation may
  // " +
  // "have been written to ignore interrupts which may result in a runaway thread. This could lead to
  // more issues, " +
  // "eventually requiring NiFi to be restarted. This is usually a bug in the target Processor '" +
  // processor +
  // "' that needs to be documented, reported and eventually fixed.");
  // }
  // }

  public CompletableFuture<Void> stop(ProcessScheduler processScheduler, ScheduledExecutorService executor,
      ProcessContext processContext, TimerDrivenSchedulingAgent schedulingAgent, LifecycleState scheduleState) {
    // TODO Auto-generated method stub
    return null;
  }

  public ScheduledState getDesiredState() {
    // TODO Auto-generated method stub
    return null;
  }


  /**
   * @param timeUnit determines the unit of time to represent the scheduling period.
   * @return the schedule period that should elapse before subsequent cycles of this processor's tasks
   */
  public long getSchedulingPeriod(final TimeUnit timeUnit) {
    return timeUnit.convert(schedulingNanos.get(), TimeUnit.NANOSECONDS);
  }

  public String toString() {
    return this.getIdentifier();
  }

  public long getYieldExpiration() {
    // TODO Auto-generated method stub
    return 0;
  }

  public boolean isTriggerWhenEmpty() {
    return this.processorRef.get().isTriggerWhenEmpty();
  }

  private long runNanos = 0L;

  public long getRunDuration(final TimeUnit timeUnit) {
    return timeUnit.convert(this.runNanos, TimeUnit.NANOSECONDS);
  }


  public static final class ActiveTask {
    private final long startTime;
    private volatile boolean terminated;

    public ActiveTask(final long startTime) {
      this.startTime = startTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public boolean isTerminated() {
      return terminated;
    }

    public void terminate() {
      this.terminated = true;
    }
  }



}
