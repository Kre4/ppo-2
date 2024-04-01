package com.itmo.microservices.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableAsync


@SpringBootApplication
@EnableAsync
class DemoServiceApplication

fun main(args: Array<String>) {
    if (System.getProperty("is.local", "false").toBoolean()) {
        println("Running locally")
    }
    runApplication<DemoServiceApplication>(*args)
}
