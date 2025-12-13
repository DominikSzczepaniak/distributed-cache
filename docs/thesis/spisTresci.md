
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

