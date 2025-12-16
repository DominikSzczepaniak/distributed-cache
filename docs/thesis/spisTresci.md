
###1. Wstęp
1.1. Wprowadzenie do systemów rozproszonych i problemu spójności danych.
1.2. Cel i zakres pracy.
1.3. Uzasadnienie wyboru technologii (Go, gRPC, Protocol Buffers).
1.4. Struktura pracy.

###2. Podstawy teoretyczne i przegląd rozwiązań
2.1. Modele spójności w systemach rozproszonych.
2.1.1. Twierdzenie CAP i kompromisy (Trade-offs).
2.1.2. Spójność silna (Linearizability) vs spójność ostateczna (Eventual Consistency).
2.2. Algorytmy konsensusu.
2.2.1. Paxos vs Raft – porównanie zrozumienia i implementacji.
2.2.2. Szczegółowa analiza algorytmu Raft (Elekcja, Replikacja, Bezpieczeństwo).
2.3. Architektury systemów Cache.
2.3.1. Sharding i Consistent Hashing.
2.3.2. Rola klienta: Thin Client vs Smart Client.
2.4. Przegląd istniejących rozwiązań (Redis Cluster, Etcd) – analiza pod kątem spójności.

###3. Analiza wymagań i projekt architektury systemu
3.1. Wymagania funkcjonalne i niefunkcjonalne (w tym odporność na partycjonowanie sieci - Network Partition Tolerance).
3.2. Architektura wysokopoziomowa.
3.2.1. Separacja Warstwy Sterowania (Control Plane) i Warstwy Danych (Data Plane).
3.2.2. Protokół komunikacyjny gRPC i definicja interfejsów (analiza pliku `raft.proto`).
3.3. Projekt modułu Control Plane.
3.3.1. Zarządzanie topologią klastra i metadanymi.
3.3.2. Mechanizm wykrywania awarii węzłów (Failure Detection / Reaper).
3.4. Projekt Smart Clienta.
3.4.1. Strategia routingu zapytań i cache'owanie topologii.
3.4.2. Obsługa ponownych prób (Retries) i mechanizmy Failover.

###4. Implementacja modułu konsensusu Raft (`pkg/raft`)
4.1. Maszyna stanów węzła Raft.
4.1.1. Implementacja ról: Follower, Candidate, Leader.
4.1.2. Mechanizm elekcji lidera (`election.go`) i obsługa kadencji (Terms).
4.2. Logika replikacji logów (`replicator.go`, `log.go`).
4.2.1. Struktura wpisu do logu (`LogEntry`) i obsługa `LogRequest` (AppendEntries).
4.2.2. Zatwierdzanie wpisów (Commit Index) i aplikowanie do maszyny stanów.
4.3. Zarządzanie trwałością danych (Persistence).
4.3.1. Zapisywanie stanu Raft na dysk (`persistance.go`).
4.3.2. Odtwarzanie stanu po restarcie węzła.
4.4. Optymalizacja rozmiaru logu – Log Compaction.
4.4.1. Mechanizm tworzenia migawek (`snapshot.go`).
4.4.2. Protokół przesyłania migawek (`InstallSnapshot` RPC).

###
5. Implementacja Control Plane i Data Plane5.1. Implementacja węzła kontrolnego (Controller).
5.1.1. Maszyna stanów topologii (`state_machine.go` w `pkg/controller`).
5.1.2. Monitorowanie zdrowia węzłów (`reaper.go`).
5.1.3. Rebalansowanie shardów (`rebalance.go`) – proces przenoszenia odpowiedzialności.
5.2. Implementacja węzła danych (DataNode).
5.2.1. Wielowątkowy magazyn danych (`ConcurrentMapCache.go`).
5.2.2. Zarządzanie dzierżawami (`lease.go`) i synchronizacja.
5.3. Implementacja klienta i biblioteki dostępowej.
5.3.1. Mechanizm `Smart Client` – obliczanie sharda po stronie klienta.
5.3.2. Zapewnienie semantyki "Exactly-once" (Idempotency Token w `raft.proto`).

###6. Weryfikacja poprawności i testy odporności (Fault Injection)
6.1. Metodyka testowania systemów rozproszonych.
6.2. Testy jednostkowe i integracyjne implementacji Raft.
6.3. Symulacja awarii i partycjonowania sieci (`tests/fault_injection`).
6.3.1. Scenariusz: Izolacja lidera (Network Partition).
6.3.2. Scenariusz: Awaria i powrót DataNode'a.
6.3.3. Weryfikacja spójności danych po awarii (`test_strong_consistency.sh`).

###7. Analiza wydajności i porównanie z istniejącymi rozwiązaniami
7.1. Środowisko testowe i metodologia pomiarów (`tests/benchmark`).
7.2. Badanie przepustowości (RPS) w zależności od liczby klientów i shardów.
7.3. Analiza narzutu protokołu Raft na czas odpowiedzi (Latency).
7.4. Studium porównawcze.
7.4.1. Porównanie wydajności z Redis (`benchmark_redis.go`).
7.4.2. Porównanie wydajności z Etcd (`benchmark_etcd.go`).
7.4.3. Wnioski z analizy porównawczej.

###8. Podsumowanie
8.1. Ocena stopnia realizacji celów pracy.
8.2. Napotkane problemy i wyzwania implementacyjne.
8.3. Kierunki dalszego rozwoju projektu.




This is a translation of a technical table of contents, likely for a thesis or project documentation, focusing on distributed systems and data consistency.

---

###1. Introduction
1.1. Introduction to distributed systems and the data consistency problem.
1.2. Aim and scope of the work.
1.3. Justification for the chosen technologies (Go, gRPC, Protocol Buffers).
1.4. Structure of the work.

---

###2. Theoretical Foundations and Overview of Solutions
2.1. Consistency models in distributed systems.
2.1.1. The CAP theorem and trade-offs.
2.1.2. Strong Consistency (Linearizability) vs. Eventual Consistency.
2.2. Consensus algorithms.
2.2.1. Paxos vs. Raft – Comparison of understanding and implementation.
2.2.2. Detailed analysis of the Raft algorithm (Election, Replication, Safety).
2.3. Cache system architectures.
2.3.1. Sharding and Consistent Hashing.
2.3.2. Role of the client: Thin Client vs. Smart Client.
2.4. Overview of existing solutions (Redis Cluster, Etcd) – Analysis in terms of consistency.

---

###3. Requirements Analysis and System Architecture Design
3.1. Functional and non-functional requirements (including Network Partition Tolerance).
3.2. High-level architecture.
3.2.1. Separation of the Control Plane and Data Plane.
3.2.2. gRPC communication protocol and interface definition (analysis of the `raft.proto` file).
3.3. Design of the Control Plane module.
3.3.1. Cluster topology and metadata management.
3.3.2. Node failure detection mechanism (Failure Detection / Reaper).
3.4. Smart Client design.
3.4.1. Query routing strategy and topology caching.
3.4.2. Handling retries and Failover mechanisms.

---

###4. Implementation of the Raft Consensus Module (`pkg/raft`)
4.1. Raft node state machine.
4.1.1. Implementation of roles: Follower, Candidate, Leader.
4.1.2. Leader election mechanism (`election.go`) and term handling (Terms).
4.2. Log replication logic (`replicator.go`, `log.go`).
4.2.1. Log entry structure (`LogEntry`) and handling of `LogRequest` (AppendEntries).
4.2.2. Committing entries (Commit Index) and applying to the state machine.
4.3. Data persistence management.
4.3.1. Writing Raft state to disk (`persistance.go`).
4.3.2. Recovering state after node restart.
4.4. Log size optimization – Log Compaction.
4.4.1. Snapshot creation mechanism (`snapshot.go`).
4.4.2. Snapshot transfer protocol (`InstallSnapshot` RPC).

---

###5. Implementation of the Control Plane and Data Plane
5.1. Implementation of the Control Node (Controller).
5.1.1. Topology state machine (`state_machine.go` in `pkg/controller`).
5.1.2. Node health monitoring (`reaper.go`).
5.1.3. Shard rebalancing (`rebalance.go`) – the process of transferring responsibility.
5.2. Implementation of the Data Node (DataNode).
5.2.1. Concurrent data store (`ConcurrentMapCache.go`).
5.2.2. Lease management (`lease.go`) and synchronization.
5.3. Implementation of the client and access library.
5.3.1. The `Smart Client` mechanism – calculating the shard on the client side.
5.3.2. Ensuring "Exactly-once" semantics (Idempotency Token in `raft.proto`).

---

###6. Correctness Verification and Fault Injection Testing
6.1. Methodology for testing distributed systems.
6.2. Unit and integration tests for the Raft implementation.
6.3. Simulation of failures and network partitioning (`tests/fault_injection`).
6.3.1. Scenario: Leader isolation (Network Partition).
6.3.2. Scenario: DataNode failure and return.
6.3.3. Verification of data consistency after failure (`test_strong_consistency.sh`).

---

###7. Performance Analysis and Comparison with Existing Solutions
7.1. Test environment and measurement methodology (`tests/benchmark`).
7.2. Throughput study (RPS) depending on the number of clients and shards.
7.3. Analysis of Raft protocol overhead on response time (Latency).
7.4. Comparative study.
7.4.1. Performance comparison with Redis (`benchmark_redis.go`).
7.4.2. Performance comparison with Etcd (`benchmark_etcd.go`).
7.4.3. Conclusions from the comparative analysis.

---

###8. Summary
8.1. Evaluation of the degree of achievement of the work's objectives.
8.2. Encountered implementation problems and challenges.
8.3. Directions for further development of the project.

---
