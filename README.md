# Kafka Share Groups vs Classic Consumer Groups — Spring Boot PoC

Reproducible experiment showing how Kafka Share Groups (KIP-932) behave when one worker is slow, compared with a traditional consumer group on **one partition**.

## Stack

Java 21 · Spring Boot 4.2.0 · Spring Integration 7.2.0 · Spring Kafka 4.2.0 · Apache Kafka 4.2.1 · Docker Compose.

## Run (Windows, Docker Desktop)

```powershell
.\scripts\run.ps1 -Mode share -SlowMs 20000 -FastMs 100
docker compose logs --no-color share-slow share-fast > share.log
python scripts/analyze.py share.log

.\scripts\run.ps1 -Mode classic -SlowMs 20000 -FastMs 100
docker compose logs --no-color classic-slow classic-fast > classic.log
python scripts/analyze.py classic.log
```

If Python is not installed: `docker run --rm -v "${PWD}:/work" -w /work python:3.13-slim python scripts/analyze.py share.log`. On PowerShell 5.1, `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` may be needed if your machine policy permits it.

On macOS/Linux: `./scripts/run.sh share` or `./scripts/run.sh classic`.

**Caution:** The runner invokes `docker compose down --volumes`, deleting Kafka data for this Compose project.

## Observed local experiment: 20 messages, single partition

| Metric | Share | Classic |
| --- | ---: | ---: |
| Completed | 20 | 20 |
| Slow worker | 1 | 1 |
| Fast worker | 19 | 19 |
| Max concurrent handlers | **2** | **1** |
| Completion-order inversions | **19** | **0** |
| First START → last DONE | **20.00 s** | **22.05 s** |
| Approx. completion rate | 1.00 msg/s | 0.91 msg/s |

The observations demonstrate parallel processing of records from a single partition in Share Groups and out-of-order *completion* in this run. A classic consumer group has only one active partition owner at a time; ownership may move between workers during a rebalance. These measurements are **not** a general throughput benchmark or an exactly-once guarantee.

## Components

- `ConsumerFlows.shareFlow()`: `ShareKafkaMessageListenerContainer`, share consumer factory, explicit acknowledgement, one-record acquisition, optional lock renewal
- `ConsumerFlows.classicFlow()`: traditional Kafka consumer adapter with one consumer thread
- `ProcessingService`: configurable slow/fast worker; logs START and DONE with message IDs and timestamps
- `Publisher`: sends one initial record and then 19 more to test slow-worker isolation
- `scripts/analyze.py`: computes worker distribution, concurrency, ordering inversions and elapsed time

## Advanced test

```powershell
.\scripts\run.ps1 -Mode share -SlowMs 40000 -RenewLocks $false
.\scripts\run.ps1 -Mode share -SlowMs 40000 -RenewLocks $true
```

Compare potential redelivery after the acquisition lock expires with automatic renewal enabled. For reliable conclusions run multiple times, preserve complete logs, and inspect broker configuration. The default handler mode used here does not itself prove exactly-once processing.

## Official references

- [Kafka KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [Spring Integration 7.2.0 announcement](https://spring.io/blog/2026/11/19/spring-integration-7-2-0-available)

## Presentation

The 12-slide PowerPoint deck includes both complete Spring Integration beans, test results, and processing-order diagrams. Upload the companion file to `presentation/kafka_share_groups_office2016_with_full_beans.pptx`.
