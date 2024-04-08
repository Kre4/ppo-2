package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream
import kotlin.streams.toList


class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
    private val secondProperties: ExternalServiceProperties,
) : PaymentExternalService {
    // создать очередь. В очереди посчитать скорость с которой мы ее разгребаем и не класть жлементы
    // именовать thread и queue
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private val scope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    }

    private val releaseJob = 0


    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newFixedThreadPool(80)

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor)
            .apply {
                maxRequests = 400
                maxRequestsPerHost = 400
            })
        connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }

    // кол-во запросов в единицу времени

    private val paymentExecutor = Executors.newFixedThreadPool(400, NamedThreadFactory("payment-executor-test"))


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        paymentExecutor.submit {
            var currentProperties = properties
            logger.warn("[${Thread.currentThread().name}][${currentProperties.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            // не смогли захватитб ресурс
            if (!currentProperties.rateLimiter.tick() /*|| !currentProperties.blockingWindow.tryAcquire()*/) {
                if (secondProperties.rateLimiter.tick()) {
                    if (secondProperties.blockingWindow.tryAcquire()) {
                        currentProperties = secondProperties
                        logger.error("Properties switches")
                    } else {
                        logger.error("No resource for window")
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "No resource for window")
                        }
                    }
                } else {
                    logger.error("No resource for ratelimit")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "No resource for ratelimit")
                    }
                }
            } else {
                //ok
                // todo проверить распределние запросов
            }

//        if (now() - paymentStartedAt > 55_000) {
//            paymentESService.update(paymentId) {
//                it.logProcessing(true, now(), transactionId, reason = "Unlucky this time, sorry")
//            }
//            return
//        }


            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${currentProperties.serviceName}&accountName=${currentProperties.accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()
            try {
                runAsyncRequest(request, transactionId, paymentId, paymentStartedAt, properties.blockingWindow)
//                runRequest(request, transactionId, paymentId, paymentStartedAt)
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
            } finally {
//            currentProperties.blockingWindow.release()
            }
        }
    }

    private fun runRequest(request: Request, transactionId: UUID, paymentId: UUID, paymentStartedAt: Long) {
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
//            blockingWindow.release()
        }
    }

    private fun runAsyncRequest(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long,
        windowToRelease: OngoingWindow
    ) {
        logger.error("Start new enqueue, dispatcher queued size: ${client.dispatcher.queuedCalls().size} " +
                "executed ${client.dispatcher.queuedCalls().stream().filter { it -> it.isExecuted() }.count()}"
        )
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                call.cancel()
                logger.error("result: Fail")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)

                }
            }

            override fun onResponse(call: Call, response: Response) {
                logger.error("result: Succ")
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    windowToRelease.release()
                } catch (e: Exception) {
                    logger.error("Critical error, transaction lost")
                    windowToRelease.release()
                }
            }
        })
    }

    private fun tryWithRateLimiterAndBulkhead(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long
    ) {
        runRequest(request, transactionId, paymentId, paymentStartedAt)
    }

}

public fun now() = System.currentTimeMillis()
