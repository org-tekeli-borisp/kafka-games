package org.tekeli.borisp.kafkagames

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaGamesApplication

fun main(args: Array<String>) {
    runApplication<KafkaGamesApplication>(*args)
}
