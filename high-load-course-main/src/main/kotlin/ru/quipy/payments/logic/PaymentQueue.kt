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
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicReferenceArray
import kotlin.math.log


// callbacks
// queue manage
class PaymentQueue(
    private val properties: List<ExternalServiceProperties>
) {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentQueue::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val paymentExecutor = Executors.newFixedThreadPool(500, NamedThreadFactory("queue-executor"))

    private val queueWatcher = Executors.newFixedThreadPool(500, NamedThreadFactory("watcher"))

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val resources: MutableList<AccountWrapper> = mutableListOf()

    // общий клиент
    private val httpClientExecutor = Executors.newFixedThreadPool(80)

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor)
            .apply {
                maxRequests = 400
                maxRequestsPerHost = 400
            })
        connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        callTimeout(80, TimeUnit.SECONDS)
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }


    private val scope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    init {
        properties.forEach {

            val queue: ArrayBlockingQueue<() -> Unit> = ArrayBlockingQueue(100)
            val wrapper = AccountWrapper(
                it, queue, ConcurrentSkipListSet(),
                scope
            )
            resources.add(wrapper)

            val job = scope.launch {
                while (true) {
                    wrapper.queue.poll()?.invoke()
                    delay(50)
                }
            }.invokeOnCompletion { th -> if (th != null) logger.error("Job completed", th) }
            val clearJob = scope.launch {
                while (true) {
                    delay(180_000)
                    wrapper.executionTimes.clear()
                }
            }
        }

    }

    fun startTransaction(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        paymentExecutor.submit {
            val transactionId: UUID = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            processTransaction(paymentId, transactionId, amount, paymentStartedAt)
        }
    }

    private fun processTransaction(paymentId: UUID, transactionId: UUID, amount: Int, paymentStartedAt: Long) {
//        val selectedProperty: AccountWrapper? = getProperties()
//        // ресурс не был захвачен,
//        if (selectedProperty == null) {
//            savePayment(paymentId, transactionId, false)
//            return
//        }
//        // в случае успеха выполняем запрос
//        runRequest(selectedProperty.property, transactionId, paymentId, paymentStartedAt)
//        logger.error("2Request process saved, duration: ${now() - paymentStartedAt}")
        putRequestInQueue(paymentId, transactionId, amount, paymentStartedAt)
    }

    private fun getProperties(): AccountWrapper? {
        for (property in resources) {
            // пробуем захват ресурса для аккаунта
            if (property.property.blockingWindow.tryAcquire() && property.property.rateLimiter.tick()) {
                // в случае успеха, иначе идем дальше по всем аккаунтам
                return property
            }
        }
        return null
    }

    private fun savePayment(paymentId: UUID, transactionId: UUID, success: Boolean, reason: String? = null) {
        val saved = paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
        logger.error("Request process saved, duration: ${saved.spentInQueueDuration}")
    }

    private fun putRequestInQueue(paymentId: UUID, transactionId: UUID, amount: Int, paymentStartedAt: Long) {
        if (now() - paymentStartedAt > 75_000) {
            savePayment(paymentId, transactionId, false)
            return
        }
        val selectedProperty: AccountWrapper? = getProperties()
        if (selectedProperty == null) {
            savePayment(paymentId, transactionId, false)
            return
        }
        selectedProperty.queue.put {
//            runRequest(selectedProperty.property, transactionId, paymentId, paymentStartedAt)
            runAsyncRequest(selectedProperty, transactionId, paymentId, paymentStartedAt)
        }
    }

    private fun runRequest(
        property: ExternalServiceProperties,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long
    ) {
        logger.error("run request")
        try {
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${property.serviceName}&accountName=${property.accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()
            logger.warn("[${property.accountName}] Start request for ${paymentId}")
            client.newCall(request).execute().use { response ->

                val body = try {
                    mapper.readValue(
                        response.body?.string(),
                        ExternalSysResponse::class.java
                    )
                } catch (e: Exception) {
                    ExternalSysResponse(false, e.message)
                }

                logger.error("Time spend on request ${now() - paymentStartedAt} ms for txId: $transactionId status: ${body.message}")
                savePayment(paymentId, transactionId, body.result, body.message)
            }
        } catch (e: Exception) {
            logger.error("Error while processing ${e.message}")
            savePayment(paymentId, transactionId, false, e.message)
        } finally {
            property.blockingWindow.release()
        }
    }

    private fun runAsyncRequest(
        account: AccountWrapper,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long,
    ) {
        logger.error(
            "Start new enqueue, dispatcher queued size: ${client.dispatcher.queuedCalls().size} " +
                    "executed ${client.dispatcher.queuedCalls().stream().filter { it -> it.isExecuted() }.count()}"
        )
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${account.property.serviceName}&accountName=${account.property.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()
        val requestStart = System.currentTimeMillis()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                updateStat(account, requestStart)
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
                    logger.error("[${account.property.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }
                updateStat(account, requestStart)
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    account.property.blockingWindow.release()
                } catch (e: Exception) {
                    logger.error("Critical error, transaction lost")
                    account.property.blockingWindow.release()
                }
            }
        })
        logger.error(
            "Avg exec time ${
                account.executionTimes.size.apply {
                    if (this != 0)
                        account.executionTimes.sum() / this
                }
            } seconds")
    }

    private fun updateStat(account: AccountWrapper, paymentStartedAt: Long) {
        account.executionTimes.add((System.currentTimeMillis() - paymentStartedAt) / 1000)
    }
}

data class AccountWrapper(
    val property: ExternalServiceProperties,
    val queue: ArrayBlockingQueue<() -> Unit>,
    val executionTimes: ConcurrentSkipListSet<Long>,
    val coroutineScope: CoroutineScope
)

data class RequestStatistic(
    val realStart: Long,
    val realEnd: Long,
)
