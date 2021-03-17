package com.m2cp.pool;

import java.sql.SQLException;
import java.util.List;

import static com.m2cp.pool.M2CPMonitor.KEY;
import static java.lang.System.currentTimeMillis;

/**
 * Utility class that runs in a new thread and carries out background tasks on the associated connection pool:
 * - reclaiming connection wrappers with expired lease
 * - shutting down the pool if all wrappers are idle
 * Properties of this utility class are immutable and must be preset before each new instance is created
 * @author mikhailsaltyshev
 * @version 1.0.2
 */
final class M2CPCleaner extends Thread
{
    // Reference to the associated pool instance
    private final M2CP targetPool;

    // Cleaner loop termination flag
    private boolean isDone = false;

    // Properties are immutable per each instance
    private final long cleanerSleep;
    private final long maxTimeLease;
    private final long maxTimeIdle;

    /**
     * Constructor to fill in the properties specifically for each instance. After an instance is created its
     * properties can't be reset
     * @param targetPool reference to the associated connection pool instance
     * @param cleanerSleep cleaner sleep time between operations in milliseconds
     * @param maxTimeLease wrapper max lease time in milliseconds before it gets reclaimed
     * @param maxTimeIdle pool max idle time in milliseconds before it gets assigned to shutdown
     */
    M2CPCleaner(M2CP targetPool, long cleanerSleep, long maxTimeLease, long maxTimeIdle) {
        this.cleanerSleep = cleanerSleep;
        this.maxTimeLease = maxTimeLease;
        this.maxTimeIdle = maxTimeIdle;
        this.targetPool = targetPool;
    }

    /**
     * Main method that gets called by the cleaner in each cycle of operations. The cleaner instance gets reference
     * to the current list of wrappers inside the associated pool and performs the following tasks:
     * - if a wrapper is currently leased, the cleaner checks its lease time. If lease time is exceeded, the cleaner
     * reclaims that wrapper by calling the {@link M2CP#removeWrapper(M2CPWrapper, boolean)} method
     * - if a wrapper is not currently leased, the cleaner increments the counter of idle wrappers, as well as marks
     * the oldest idle wrapper. If the counter equals the size of the list of wrappers (which means that all wrappers
     * are idle) and if the oldest idle wrapper has exceeded max idle time, the cleaner starts the pool shutdown
     * procedure by calling {@link M2CP#shutdown()} method
     * - if the list of wrappers is empty, the cleaner sets its loop termination flag to true and ceases activity
     * @see M2CP#returnConnection(M2CPWrapper)
     */
    public void performCleaning() {
        synchronized (KEY) {

            int idleCounter = 0;
            long longestIdle = 0;

            List<M2CPWrapper> wrapperList = targetPool.getWrapperList();

            for (M2CPWrapper wrapper : wrapperList) {
                if (wrapper.isLeased()) {
                    // Reclaim a wrapper with expired lease
                    if (maxTimeLease >= 0 && wrapper.getLastTimeLeased() < (currentTimeMillis() - maxTimeLease)) {
                        try {
                            targetPool.removeWrapper(wrapper, true);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    // Increment counter if a wrapper is idle
                    idleCounter++;

                    // Mark the oldest idle wrapper
                    if (wrapper.getLastTimeReturned() > longestIdle) {
                        longestIdle = wrapper.getLastTimeReturned();
                    }
                }

                // Shutdown the pool if all wrappers are idle and the oldest one exceeds max idle time
                if (idleCounter == wrapperList.size() && longestIdle < (currentTimeMillis() - maxTimeIdle)) {
                    try {
                        targetPool.shutdown();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            // If the list is empty, stop the cleaner
            if (wrapperList.isEmpty()) {
                isDone = true;
            }
        }
    }

    /**
     * Cleaner thread run method. The cleaner performs its tasks, goes to sleep, then wakes up and checks if the
     * list of wrappers is not empty. If the list is empty, the cleaner exits the loop and ceases activity
     */
    @Override
    public void run() {
        while (!isDone) {
            performCleaning();

            try {
                //noinspection BusyWait
                Thread.sleep(cleanerSleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (isDone) {
                break;
            }
        }
    }
}
