package com.m2cp.pool;

/**
 * This class is used as a monitor for synchronizing operations between the pool, the cleaner and the user threads
 * @author mikhailsaltyshev
 * @version 1.0.2
 */
final class M2CPMonitor
{
    // Use this constant as a monitor key
    static final Object KEY = new Object();

    private M2CPMonitor() {}
}
