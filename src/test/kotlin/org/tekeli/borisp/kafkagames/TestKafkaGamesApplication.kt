package org.tekeli.borisp.kafkagames

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<KafkaGamesApplication>().with(TestcontainersConfiguration::class).run(*args)
}
