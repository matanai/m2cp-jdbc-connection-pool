package com.m2cp.pool;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * The wrapper class for {@link Connection} implementation. All calls to a wrapped connection are substituted with
 * methods defined in this class. The connection object inside the wrapper is designed to be associated only with
 * that particular wrapper. Should the connection object be closed or invalidated, the connection gets closed and
 * its wrapper is destroyed
 * @author mikhailsaltyshev
 * @version 1.0.2
 */
public final class M2CPWrapper implements Connection
{
    // An actual instance of java.sql.Connection implementation
    private final Connection realConnection;

    // Wrapper properties
    private boolean leased = false;
    private long lastTimeLeased = 0;
    private long lastTimeReturned = 0;

    /**
     * Constructor for a wrapper instance that substitutes actual {@link Connection} implementation instance
     * @param realConnection an instance of {@link Connection} implementation
     */
    M2CPWrapper(Connection realConnection) {
        this.realConnection = realConnection;
    }

    /**
     * Gets current lease status of the wrapper
     * @return true if the wrapper is currently leased; false otherwise
     */
    public boolean isLeased() {
        return leased;
    }

    /**
     * Gets lease status of the wrapper
     * @param used true if the wrapper is currently leased; false otherwise
     */
    void setLeased(boolean used) {
        this.leased = used;
    }

    /**
     * Gets the last time this wrapper was leased
     * @return time in milliseconds
     */
    long getLastTimeLeased() {
        return lastTimeLeased;
    }

    /**
     * Sets the last time this wrapper was leased
     * @param lastTimeLeased time in milliseconds
     */
    void setLastTimeLeased(long lastTimeLeased) {
        this.lastTimeLeased = lastTimeLeased;
    }

    /**
     * Gets the last time this wrapper was returned to the pool
     * @return time in milliseconds
     */
    long getLastTimeReturned() {
        return lastTimeReturned;
    }

    /**
     * Sets the last time this wrapper was returned to the pool
     * @param lastTimeReturned time in milliseconds
     */
    void setLastTimeReturned(long lastTimeReturned) {
        this.lastTimeReturned = lastTimeReturned;
    }

    /**
     * This method is a wrapper for the actual {@link Connection#close()} method that is called when the associated
     * connection inside the wrapper object must be closed.
     * @throws SQLException if {@link Connection#close} method fails
     */
    void closeRealConnection() throws SQLException {
        realConnection.close();
    }

    /**
     * This method redirects the call to the {@link M2CP#returnConnection(M2CPWrapper)}, which returns the wrapper
     * back to the pool. A check is performed whether the pool instance is not null to prevent situations when the
     * user app tries to return a wrapper to the non-existing pool
     * @throws SQLException if {@link M2CP#returnConnection(M2CPWrapper)} method fails
     */
    @Override
    public void close() throws SQLException {
        synchronized (M2CPMonitor.KEY) {
            if (M2CP.pool != null) {
                M2CP.pool.returnConnection(this);
            }
        }
    }

    // Following are the original methods from java.sql.Connection interface

    @Override
    public Statement createStatement() throws SQLException {
        return realConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return realConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return realConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return realConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        realConnection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return realConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        realConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        realConnection.rollback();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return realConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return realConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        realConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return realConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        realConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return realConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        realConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return realConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return realConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        realConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return realConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return realConnection.prepareStatement(sql, resultSetType,resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return realConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        realConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        realConnection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return realConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return realConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return realConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        realConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        realConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return realConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return realConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return realConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return realConnection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return realConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return realConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return realConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return realConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return realConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return realConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return realConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        realConnection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        realConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return realConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return realConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return realConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return realConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        realConnection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return realConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        realConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        realConnection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return realConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return realConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return realConnection.isWrapperFor(iface);
    }
}
