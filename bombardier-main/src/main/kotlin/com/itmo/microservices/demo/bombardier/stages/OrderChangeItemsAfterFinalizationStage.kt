package com.itmo.microservices.demo.bombardier.stages

import com.itmo.microservices.commonlib.annotations.InjectEventLogger
import com.itmo.microservices.commonlib.logging.EventLogger
import com.itmo.microservices.demo.bombardier.flow.CoroutineLoggingFactory
import com.itmo.microservices.demo.bombardier.external.OrderStatus
import com.itmo.microservices.demo.bombardier.external.ExternalServiceApi
import com.itmo.microservices.demo.bombardier.flow.UserManagement
import com.itmo.microservices.demo.bombardier.logging.OrderChangeItemsAfterFinalizationNotableEvents.*
import com.itmo.microservices.demo.bombardier.utils.ConditionAwaiter
import com.itmo.microservices.demo.common.logging.EventLoggerWrapper
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Component
class OrderChangeItemsAfterFinalizationStage : TestStage {
    @InjectEventLogger
    lateinit var eventLog: EventLogger

    lateinit var eventLogger: EventLoggerWrapper

    override suspend fun run(userManagement: UserManagement, externalServiceApi: ExternalServiceApi): TestStage.TestContinuationType {
        eventLogger = EventLoggerWrapper(eventLog, testCtx().serviceName)
        val shouldRunStage = Random.nextBoolean()
        if (!shouldRunStage) {
            eventLogger.info(I_STATE_SKIPPED, testCtx().orderId)
            testCtx().wasChangedAfterFinalization = false
            return TestStage.TestContinuationType.CONTINUE
        }

        testCtx().wasChangedAfterFinalization = true
        eventLogger.info(I_START_CHANGING_ITEMS, testCtx().orderId)

        val items = externalServiceApi.getAvailableItems(testCtx().userId!!)
        repeat(Random.nextInt(1, 5)) {
            val itemToAdd = items.random()

            val amount = Random.nextInt(1, 13)
            externalServiceApi.putItemToOrder(testCtx().userId!!, testCtx().orderId!!, itemToAdd.id, amount)

            ConditionAwaiter.awaitAtMost(3, TimeUnit.SECONDS)
                .condition {
                    val theOrder = externalServiceApi.getOrder(testCtx().userId!!, testCtx().orderId!!)
                    theOrder.itemsMap.any { it.key == itemToAdd.id && it.value == amount }
                            && theOrder.status == OrderStatus.OrderCollecting
                }
                .onFailure {
                    eventLogger.error(E_ORDER_CHANGE_AFTER_FINALIZATION_FAILED, itemToAdd.id, amount, testCtx().orderId)
                    if (it != null) {
                        throw it
                    }
                    throw TestStage.TestStageFailedException("Exception instead of silently fail")
                }.startWaiting()
        }

        return TestStage.TestContinuationType.CONTINUE
    }
}