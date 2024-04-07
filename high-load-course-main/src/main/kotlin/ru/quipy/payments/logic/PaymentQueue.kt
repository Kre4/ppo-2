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
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// callbacks
// queue manage
class PaymentQueue(
    private val properties: List<ExternalServiceProperties>
) {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentQueue::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val delta = 10_000
        val requestsQueueSizeLimit: Int = 3000
        // Посчитать объем V/W, где V объем очереди который можем позволить. Пусть на каждую 307Mb ~ 307_000_000B
        // Элемент 100B -> можем иметь очередь на 3млн элементов, правда она нам не нужна

        // Или считаем, что нужно хранить стэк, т.е 1mb, тогда 38
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

            val queue: ArrayBlockingQueue<ExecutableWithTimeStamp> = ArrayBlockingQueue(100)
            val wrapper = AccountWrapper(
                it, queue, ConcurrentSkipListSet(),
                scope
            )
            resources.add(wrapper)

            val job = scope.launch {
                while (true) {
                    wrapper.queue.poll()?.executable?.invoke()
                    delay(50)
                }
            }.invokeOnCompletion { th -> if (th != null) logger.error("Job completed", th) }
            val clearJob = scope.launch {
                while (true) {
                    delay(20_000)
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
        logger.error("Saving payment succ: $success, reason $reason for tx ${transactionId} and pId ${paymentId}")
        val saved = paymentESService.update(paymentId) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
    }

    private fun putRequestInQueue(paymentId: UUID, transactionId: UUID, amount: Int, paymentStartedAt: Long) {

        // если уже вы
        if (now() - paymentStartedAt > 75_000) {
            savePayment(paymentId, transactionId, false, "Process started too late")
            return
        }
        val selectedProperty: AccountWrapper? = getProperties()

        // не смогли получить ресурсы
        if (selectedProperty == null) {
            savePayment(paymentId, transactionId, false, "No free resource")
            return
        }

        // а не переполнена ли очередь?
        val currentQueueSize = selectedProperty.queue.size
        if (currentQueueSize > requestsQueueSizeLimit) {
            while (System.currentTimeMillis() - selectedProperty.queue.poll().paymentStartedAt > 80_000) {
            }
        }

        // после разгона прибавлять вес среднему значению
        val predictedTimeMillis = client.dispatcher.queuedCallsCount() / 80 * 10_000

        if (System.currentTimeMillis() - paymentStartedAt + predictedTimeMillis > 80_000) {
            savePayment(paymentId, transactionId, false, "predicted more than 80s")
            return
        }
        selectedProperty.queue.put(
            ExecutableWithTimeStamp(paymentStartedAt) {
                runAsyncRequest(selectedProperty, transactionId, paymentId, paymentStartedAt)
            })
    }

    private fun runAsyncRequest(
        account: AccountWrapper, // 8 bytes
        transactionId: UUID, // 16 bytes
        paymentId: UUID, // 16 bytes
        paymentStartedAt: Long // 8 bytes, -> total 48 bytes
    ) {
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${account.property.serviceName}&accountName=${account.property.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()
        val requestStart = System.currentTimeMillis()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                updateStat(account, requestStart)
                call.cancel()
                savePayment(paymentId, transactionId, false, e.message)
//                paymentESService.update(paymentId) {
//                    it.logProcessing(false, now(), transactionId, reason = e.message)
//
//                }
            }

            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    ExternalSysResponse(false, e.message)
                }
                updateStat(account, requestStart)
                try {
                    savePayment(paymentId, transactionId, body.result, body.message)
//                    paymentESService.update(paymentId) {
//                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                    }
                    account.property.blockingWindow.release()
                } catch (e: Exception) {
                    account.property.blockingWindow.release()
                }
            }
        })

    }

    private fun getAvgExecTimeForAccount(account: AccountWrapper): Long {
        // TODO просмотреть содержимое массива
        val sum = account.executionTimes.sum()
        val size = account.executionTimes.size
        return if (size != 0) {
            sum / size
        } else {
            0
        }
    }

    private fun updateStat(account: AccountWrapper, paymentStartedAt: Long) {
        account.executionTimes.add((System.currentTimeMillis() - paymentStartedAt))
    }
}

data class AccountWrapper(
    val property: ExternalServiceProperties,
    val queue: ArrayBlockingQueue<ExecutableWithTimeStamp>,
    val executionTimes: ConcurrentSkipListSet<Long>, // TODO заменить skipListSet
    val coroutineScope: CoroutineScope
)

data class ExecutableWithTimeStamp(
    val paymentStartedAt: Long,
    val executable: () -> Unit,
)
