package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"
        const val SECONDARY_PAYMENT_BEAN = "SECONDARY_PAYMENT_BEAN"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

        private val accountProps_1 = ExternalServiceProperties(
            // most expensive. Call costs 100
            "test",
            "default-1",
            RateLimiter(100),
            OngoingWindow(1000),
            NonBlockingOngoingWindow(1000),
            1000)

        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            RateLimiter(100),
            OngoingWindow(1000),
            NonBlockingOngoingWindow(1000),
            10_000
        )

//        private val accountProps_3 = ExternalServiceProperties(
//            // Call costs 40
//            "test",
//            "default-3",
//        )
//
//        // Call costs 30
//        private val accountProps_4 = ExternalServiceProperties(
//            "test",
//            "default-4",
//        )
    }

    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService() =
        PaymentExternalServiceImpl(
            accountProps_2,
            accountProps_1
        )

//    @Bean(SECONDARY_PAYMENT_BEAN)
//    fun reserveExternalService() =
//        PaymentExternalServiceImpl(
//            accountProps_1,
//        )
}
