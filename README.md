# M2CP Connection Pool

A simple, lightweight JDBC connection pool, built mainly for learning purposes.      

## Index :pushpin:
- [About the project](#about)
- [Instructions](#instructions)
- [License](#license)

## About the project <a name="about"></a> :link:

In this project I've tried to implement all common ideas used when designing a connection pool, e.g. a wrapper class that takes care of all interactions with an actual connection, automatic reclaim and shut down procedures, concurrency support, etc.

#### Key features 

- Support for up to 100 connections in the pool
- Automatic reclaim and shut down procedures
- Simple API for accessing settings
- Thread-safe

## Instructions <a name="instructions"></a> :wrench:

To start the connection pool just call `getConnection()` from the `M2CP` class. The pool gets automatically shut down when all connections have been idle for a certain amount of time (can be defined in the settings). You may even ignore calling `close()` method on a connection, as the pool will automatically reclaim each connection that has been used for too long (can also be defined in the settings). However, it is still a better practice to call `close()` method manually or via try-with-resources, as it will force the pool to reclaim the connection immediately

```java
    public static void main(String[] args) {
        try (Connection connection = M2CP.getConnection(url, username, password)) {
            // Your code 
        } catch (SQLException e) {
            // Your code
        }
    } 
```

Before starting the pool you may want to tweak some of the pool settings that will be applied to the next pool instance.

```java
    M2CP.setPoolSize(100);
    M2CP.setCleanerSleep(500);
    M2CP.setMaxTimeLease(3000);
    M2CP.setMaxTimeIdle(5000);
```

If none of these methods were called, the pool will apply default settings

#### Requirements

- Java SE 1.8 or higher

## License <a name="license"></a> :clipboard:

This program is released under an MIT License
