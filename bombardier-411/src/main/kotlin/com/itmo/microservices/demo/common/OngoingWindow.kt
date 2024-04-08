package com.itmo.microservices.demo.common

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    private val maxWinSize: Int
) {
    private val winSize = AtomicInteger()
 
    fun putIntoWindow(): WindowResponse {
        while (true) {
            val currentWinSize = winSize.get()
            if (currentWinSize >= maxWinSize) {
                return WindowResponse.Fail(currentWinSize)
            }
 
            if (winSize.compareAndSet(currentWinSize, currentWinSize + 1)) {
                break
            }
        }
        return WindowResponse.Success(winSize.get())
    }
 
    fun releaseWindow() = winSize.decrementAndGet()
 
 
    sealed class WindowResponse(val currentWinSize: Int) {
        public class Success(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
 
        public class Fail(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
    }
}

class SemaphoreOngoingWindow(
    val maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    suspend fun acquire(): Boolean {
        return try {
            withTimeout(1000) {
                window.acquire()
                true
            }
        } catch (e: TimeoutCancellationException) {
            false
        }
    }

    fun release() = window.release()
}