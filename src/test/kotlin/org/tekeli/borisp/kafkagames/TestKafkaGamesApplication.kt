package org.tekeli.borisp.kafkagames

import org.springframework.boot.fromApplication


fun main(args: Array<String>) {
    fromApplication<KafkaGamesApplication>().run(*args)
}
