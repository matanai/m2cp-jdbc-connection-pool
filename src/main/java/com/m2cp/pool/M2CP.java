package com.m2cp.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.m2cp.pool.M2CPMonitor.KEY;
import static java.lang.System.currentTimeMillis;

/**
 * Class that represents a connection pool. Pool gets populated with wrapper objects that hold actual connections.
 * The term "lease" is used hereafter to indicate that the {@link M2CPWrapper} object is not meant to be actually
 * closed, but simply returned back to the pool after its usage.
 * The {@link M2CP} class is designed to hold only one instance of the pool at a time. Each property of the class
 * (except for references to the current pool instance and the list of wrappers, both of which are associated with
 * the particular pool instance), are class member variables that are set globally before a new instance of the pool
 * is created. The {@link M2CPCleaner} object is built per each pool instance to perform utility operations in a
 * separate thread while also controlling, when that pool instance must be shut down. The general idea is that the
 * pool gets shut down automatically when all wrappers in the list have been idle for a certain amount of time
 * @author mikhailsaltyshev
 * @version 1.0.2
 */
public final class M2CP
{
    // Connection pool instance
    static volatile M2CP pool;

    // Database access properties
    private static String url;
    private static String username;
    private static String password;

    // Global settings applied to each new pool instance
    private static int poolSize = 10;

    private static long cleanerSleep = 1000;
    private static long maxTimeLease = 1000;
    private static long maxTimeIdle = 1000;

    // Collection to hold all connection wrapper objects
    private final List<M2CPWrapper> wrapperList = new CopyOnWriteArrayList<>();

    /**
     * Private constructor to prevent instantiating this pool from outside the class. This constructor populates
     * the {@link List} collection with wrapper objects, holding associated connections
     * @throws SQLException if the invoked {@link #getRealConnection()} fails to get connection
     */
    private M2CP() throws SQLException {
        repopulateWrapperList();
    }

    /**
     * Sets the cleaner sleep time between operations on this pool instance. The property will not take effect if
     * a pool instance has already been created. It is not recommended to set sleep time lower than 10 milliseconds,
     * as it can have negative impact on performance and result in errors
     * @param cleanerSleep in milliseconds (default is 1000)
     */
    public static void setCleanerSleep(long cleanerSleep) {
        M2CP.cleanerSleep = cleanerSleep;
    }

    /**
     * Sets a connection max lease time before it gets reclaimed by the cleaner. The property will not take effect
     * if a pool instance has already been created
     * @param maxTimeLease in milliseconds (default is 1000)
     */
    public static void setMaxTimeLease(long maxTimeLease) {
        M2CP.maxTimeLease = maxTimeLease;
    }

    /**
     * Sets pool max idle time before it gets assigned to shut down by the cleaner. The property will not take effect
     * if a pool instance has already been created
     * @param maxTimeIdle in milliseconds (default is 1000)
     */
    public static void setMaxTimeIdle(long maxTimeIdle) {
        M2CP.maxTimeIdle = maxTimeIdle;
    }

    /**
     * Sets pool size, i.e. max number of open connections in this pool. The property will not take effect if a pool
     * instance has already been created
     * @param poolSize max number of open connections in this pool (default is 10)
     */
    public static void setPoolSize(int poolSize) {
        if (pool == null) {
            M2CP.poolSize = poolSize;
        }
    }

    /**
     * The starter method to be called from outside the package. This method is used to get the wrapper object from
     * the pool, delegating the operation to a {@link #leaseConnection()} method. Before the method gets called this
     * method checks if the pool was actually created, and if not, instantiates the pool in the first place
     * @param url string to access the database
     * @param username string to access the database
     * @param password string to access the database
     * @return an instance of {@link M2CPWrapper} class
     * @throws SQLException if the invoked {@link #getRealConnection()} fails to acquire connection
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        synchronized (KEY) {
            if (pool == null) {

                M2CP.url = url;
                M2CP.username = username;
                M2CP.password = password;

                // Zero or negative values are not accepted
                if (cleanerSleep < 1 || maxTimeLease < 1 || maxTimeIdle < 1) {
                    throw new M2CPException("Failed to initialize cleaner: "
                            + "time values must be positive integers higher than zero");
                }

                if (poolSize < 1 || poolSize > 100) {
                    throw new M2CPException("Failed to initialize pool: "
                            + "pool size must be a positive integer between 1 and 100");
                }

                // If OK, create new pool
                pool = new M2CP();

                // Assign new utility instance to this pool and run it in a separate thread
                new M2CPCleaner(pool, cleanerSleep, maxTimeLease, maxTimeIdle).start();
            }
            return pool.leaseConnection();
        }
    }

    /**
     * The method retrieves the first available instance of a wrapper from the pool and returns it to the user app.
     * Before leasing the wrapper instance, the method checks if a wrapped connection object is not closed and is
     * still valid. Otherwise the method calls {@link #removeWrapper(M2CPWrapper, boolean)} method to replace the
     * wrapper. If there are no available wrappers, the method throws unchecked exception
     * @return an instance of {@link M2CPWrapper} class
     */
    Connection leaseConnection() {
        for (M2CPWrapper wrapper : wrapperList) {
            if (!wrapper.isLeased()) {
                try {
                    // Replace the wrapper if its payload is closed or invalid
                    if (wrapper.isClosed() || wrapper.getWarnings() != null) {
                        removeWrapper(wrapper, true);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                wrapper.setLastTimeLeased(currentTimeMillis());
                wrapper.setLeased(true);
                return wrapper;
            }
        }
        throw new M2CPException("Failed to lease connection: no available connections");
    }

    /**
     * This method is used to return the leased wrapper instance back to the pool. The method also sets auto-commit
     * property to default. Two nested if-statements are required to prevent errors when the user app has exceeded
     * lease and tries to return a wrapper that has already been reclaimed by the cleaner. In this case the method
     * simply ignores an overdue wrapper. All other cases indicate that the user app is trying to return a wrapper
     * that is not part of this pool instance, which is addressed by throwing an unchecked exception
     * @param wrapper an instance of {@link M2CPWrapper} class
     * @throws SQLException if {@link Connection#setAutoCommit(boolean)} fails
     */
    void returnConnection(M2CPWrapper wrapper) throws SQLException {
        if (wrapper.isLeased()) {
            // Nested if-statements are required to address overdue wrappers
            if (wrapperList.contains(wrapper)) {
                wrapper.setLastTimeReturned(currentTimeMillis());
                wrapper.setAutoCommit(true);
                wrapper.setLeased(false);
            }
        } else {
            throw new M2CPException("Failed to return connection: connection is not part of this pool");
        }
    }

    /**
     * This method gets reference to the current list of wrappers, associated with the pool instance. The method is
     * designed to be called mainly by the cleaner instance
     * @return reference to the current list of wrappers
     */
    List<M2CPWrapper> getWrapperList() {
        return wrapperList;
    }

    /**
     * This method removes a wrapper by closing the associated connection and removing the wrapper from the list.
     * Optionally the caller may indicate that the list must be repopulated after the wrapper has been removed
     * @param wrapper an instance of {@link M2CPWrapper} class to be removed
     * @param repopulate option if the list must be repopulated with fresh wrappers
     * @throws SQLException if {@link M2CPWrapper#closeRealConnection()} fails to close the connection
     */
    void removeWrapper(M2CPWrapper wrapper, boolean repopulate) throws SQLException {
        wrapper.closeRealConnection();
        wrapperList.remove(wrapper);

        if (repopulate) {
            repopulateWrapperList();
        }
    }

    /**
     * This method shuts down this pool instance by first checking that the pool exists and the list of wrappers is
     * not empty, and then removing each wrapper in the list by calling {@link #removeWrapper(M2CPWrapper, boolean)}
     * method. When the list is empty, the method sets current pool reference to null
     * @throws SQLException if {@link #removeWrapper(M2CPWrapper, boolean)} fails
     */
    void shutdown() throws SQLException {
        if (pool != null && !(wrapperList.isEmpty())) {
            for (M2CPWrapper wrapper : wrapperList) {
                removeWrapper(wrapper, false);
            }
            pool = null;
        }
    }

    /**
     * The method repopulates the list of wrappers by creating new instances of the wrapper class. To determine the
     * number of wrappers to be added, the method checks divergence between the requested size of the pool and the
     * actual size of the current list of wrappers. Divergence other than zero means that some wrappers were removed,
     * and the list must be repopulated. If there is a divergence, the method will repopulate the list with fresh
     * wrappers while not exceeding the requested pool size
     * @throws SQLException if the method fails to get connection
     */
    private void repopulateWrapperList() throws SQLException {
        int divergence = poolSize - wrapperList.size();
        if (divergence != 0) {
            for (int i = 0; i < divergence; i++) {
                wrapperList.add(new M2CPWrapper(getRealConnection()));
            }
        }
    }

    /**
     * This method gets an actual {@link Connection} implementation to be wrapped inside a wrapper object
     * @return an instance of {@link Connection} implementation
     * @throws SQLException if the method fails to get connection
     */
    private Connection getRealConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
