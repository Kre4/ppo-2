package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val window = NonBlockingOngoingWindow(1000) // вопрос размерности
        val windowTemporary = NonBlockingOngoingWindow(1000)
        val blockingWindow: OngoingWindow = OngoingWindow(100000)

        val nonThreadPoolBulkhead = BulkheadConfig.custom()
            .maxConcurrentCalls(150)
            .maxWaitDuration(Duration.ofMillis(500))
            .build();
        var bulkheadConfig: ThreadPoolBulkheadConfig = ThreadPoolBulkheadConfig.custom()
            .maxThreadPoolSize(2000)
            .coreThreadPoolSize(2000)
            .queueCapacity(200)
            .build()
        var bulkhead = Bulkhead.of("myBulkhead", nonThreadPoolBulkhead)

        private val rateLimiterConfig: RateLimiterConfig = RateLimiterConfig.custom()
            .limitForPeriod(100)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(Duration.ofMillis(5))
            .build()

        private val rateLimiterRegistry: RateLimiterRegistry = RateLimiterRegistry.of(rateLimiterConfig)

        val rateLimiter: RateLimiter = rateLimiterRegistry.rateLimiter("myRateLimiter")

    }
//
//    val rateLimit = RateLimiterRegistry.of(
//        RateLimiterConfig.custom()
//            .limitRefreshPeriod(Duration.ofSeconds(1))
//            .limitForPeriod(15)
//            .timeoutDuration(Duration.ofMillis(25))
//            .build()
//    ).rateLimiter("testRateLimit")

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (now() - paymentStartedAt > 55_000) {
            paymentESService.update(paymentId) {
                it.logProcessing(true, now(), transactionId, reason = "Unlucky this time, sorry")
            }
            logger.error("Since start (but error) ${System.currentTimeMillis() - paymentStartedAt} ms")
            return
        }


        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()
        try {
            tryWithRateLimiterAndBulkhead(request, transactionId, paymentId, paymentStartedAt)
//            paymentExecutor.submit {
//                when (window.putIntoWindow()) {
//                    is NonBlockingOngoingWindow.WindowResponse.Success -> {
//                        logger.error("Window success")
//                        runAsyncRequest(request, transactionId, paymentId, paymentStartedAt, window)
//                    }
//
//                    is NonBlockingOngoingWindow.WindowResponse.Fail -> {
//                        logger.error("Window fail, let's try another one")
//                        //TODO сделать нормально
//                        val request = Request.Builder().run {
//                            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=default-1&transactionId=$transactionId")
//                            post(emptyBody)
//                        }.build()
//                        when (windowTemporary.putIntoWindow()) {
//                            is NonBlockingOngoingWindow.WindowResponse.Success -> {
//                                logger.error("Window success")
//                                runAsyncRequest(request, transactionId, paymentId, paymentStartedAt, windowTemporary)
//                            }
//
//                            is NonBlockingOngoingWindow.WindowResponse.Fail -> {
//                                logger.error("Window fail, final")
//                                paymentESService.update(paymentId) {
//                                    it.logProcessing(false, now(), transactionId, reason = "Window fail")
//                                }
//                            }
//                        }
//
//                    }
//                }
//            }
            // TODO отследить эффекты каждого изменения
            // проверить индивидуальный подход выделения окна
            // поискать багу, попробовать execute вместо enqueue но с окном и ratelimit ( + бесконечное окно)
            // еще раз запуск до любой оптимизации
            //
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    private fun runBlockingRequest(request: Request, transactionId: UUID, paymentId: UUID, paymentStartedAt: Long) {
        client.newCall(request).execute().use { response ->

            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(false, e.message)
            }

            logger.error("Time spend on request ${now() - paymentStartedAt} ms for txId: $transactionId status: ${body.message}")
            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
            logger.error("Since start ${System.currentTimeMillis() - paymentStartedAt} ms")
            blockingWindow.release()
        }
    }

    private fun runAsyncRequest(request: Request, transactionId: UUID, paymentId: UUID, paymentStartedAt: Long) {
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {

                logger.error("onFailure")
                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: false, message: ${e.message}")
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment failed due to SocketTimeoutException for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
                blockingWindow.release()
            }

            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                blockingWindow.release()
            }
        })
    }

    private fun tryWithRateLimiterAndBulkhead(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long
    ) {
//        val supplier: CompletionStage<Unit> = ThreadPoolBulkhead.decorateSupplier(bulkhead, runAsyncRequest(request, transactionId, paymentId, paymentStartedAt))
//        val decoratedCallable = ThreadPoolBulkhead
//            .decorateCallable(bulkhead, RateLimiter.decorateCallable(rateLimiter) {
//                runBlockingRequest(request, transactionId, paymentId, paymentStartedAt)
//            })
//        val result = decoratedCallable.get()
//        RateLimiter.decorateCallable(rateLimiter) {
        var isPermitted = false
        while (!isPermitted) {
            isPermitted = blockingWindow.acquire()
            if (isPermitted) {
                runBlockingRequest(request, transactionId, paymentId, paymentStartedAt)
            } else {
                logger.warn("Permission blocked")
            }
        }
//        }.call()
// todo ulkheadFullException
//        try {
        // Execute the decorated callable

//        } catch (e: Exception) {
//            logger.error("Error while rate+bulk ${e.message}")
//            paymentESService.update(paymentId) {
//                it.logProcessing(false, now(), transactionId, reason = e.message)
//            }
//        }

    }
}

public fun now() = System.currentTimeMillis()
