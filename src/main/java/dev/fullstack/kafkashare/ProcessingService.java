package dev.fullstack.kafkashare;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "poc.role", havingValue = "consumer")
class ProcessingService {
    private final String mode;
    private final String workerId;
    private final long delayMs;

    ProcessingService(@Value("${poc.mode}") String mode,
                      @Value("${poc.worker-id}") String workerId,
                      @Value("${poc.delay-ms}") long delayMs) {
        this.mode = mode;
        this.workerId = workerId;
        this.delayMs = delayMs;
    }

    public void process(Message<?> message) {
        String id = String.valueOf(message.getPayload());
        Object partition = message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION);
        Object offset = message.getHeaders().get(KafkaHeaders.OFFSET);
        Instant began = Instant.now();
        emit("START", id, partition, offset, began, 0);
        try {
            // A deliberately blocking business handler, to test head-of-line blocking.
            Thread.sleep(delayMs);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            emit("ERROR", id, partition, offset, Instant.now(), 0);
            throw new IllegalStateException("Interrupted during " + id, ex);
        }
        Instant ended = Instant.now();
        emit("DONE", id, partition, offset, ended,
                ended.toEpochMilli() - began.toEpochMilli());
    }

    private void emit(String event, String id, Object partition, Object offset,
                      Instant at, long elapsedMs) {
        System.out.printf(
            "POC EVENT=%s MODE=%s WORKER=%s ID=%s PARTITION=%s OFFSET=%s AT=%s DURATION_MS=%d%n",
            event, mode, workerId, id, partition, offset, at, elapsedMs);
    }
}
