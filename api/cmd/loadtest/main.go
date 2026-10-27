package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	targetURL := flag.String("url", "http://api:8080/health", "Target HTTP endpoint")
	totalReqs := flag.Int("n", 5000, "Total number of HTTP requests")
	concurrency := flag.Int("c", 50, "Number of concurrent workers")
	flag.Parse()

	fmt.Printf("============================================================\n")
	fmt.Printf("OPENTELEMETRY SATURATION LOAD TEST (Collector Unavailable)\n")
	fmt.Printf("Target: %s | Total: %d | Concurrency: %d\n", *targetURL, *totalReqs, *concurrency)
	fmt.Printf("============================================================\n")

	tr := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	// Warmup check
	resp, err := client.Get(*targetURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Target unreachable during warmup: %v\n", err)
		os.Exit(1)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Target returned non-200 status during warmup: %d\n", resp.StatusCode)
		os.Exit(1)
	}
	fmt.Println("✔ Target is reachable and returning HTTP 200.")

	reqsPerWorker := *totalReqs / *concurrency
	remainder := *totalReqs % *concurrency

	var successCount atomic.Int64
	var errorCount atomic.Int64

	latencies := make([]time.Duration, *totalReqs)
	var latenciesIdx atomic.Int64

	startGate := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < *concurrency; w++ {
		count := reqsPerWorker
		if w == 0 {
			count += remainder
		}

		wg.Add(1)
		go func(workerID, nReqs int) {
			defer wg.Done()
			<-startGate // Synchronized barrier start

			for i := 0; i < nReqs; i++ {
				t0 := time.Now()
				req, _ := http.NewRequest(http.MethodGet, *targetURL, nil)
				req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

				res, err := client.Do(req)
				dur := time.Since(t0)

				idx := latenciesIdx.Add(1) - 1
				if idx < int64(len(latencies)) {
					latencies[idx] = dur
				}

				if err == nil && res.StatusCode == http.StatusOK {
					res.Body.Close()
					successCount.Add(1)
				} else {
					if res != nil && res.Body != nil {
						res.Body.Close()
					}
					errorCount.Add(1)
				}
			}
		}(w, count)
	}

	testStart := time.Now()
	close(startGate) // Release all workers concurrently
	wg.Wait()
	testDuration := time.Since(testStart)

	actualTotal := successCount.Load() + errorCount.Load()
	validLatencies := latencies[:actualTotal]
	sort.Slice(validLatencies, func(i, j int) bool {
		return validLatencies[i] < validLatencies[j]
	})

	var totalLatency time.Duration
	for _, l := range validLatencies {
		totalLatency += l
	}

	p50 := validLatencies[len(validLatencies)*50/100]
	p90 := validLatencies[len(validLatencies)*90/100]
	p95 := validLatencies[len(validLatencies)*95/100]
	p99 := validLatencies[len(validLatencies)*99/100]
	maxLat := validLatencies[len(validLatencies)-1]
	minLat := validLatencies[0]
	avgLat := totalLatency / time.Duration(len(validLatencies))
	rps := float64(actualTotal) / testDuration.Seconds()

	fmt.Printf("\n--- RESULTADOS DO TESTE DE SATURAÇÃO ---\n")
	fmt.Printf("Duração Total:        %v\n", testDuration)
	fmt.Printf("Throughput (RPS):     %.2f reqs/s\n", rps)
	fmt.Printf("Sucessos (HTTP 200):  %d (%.2f%%)\n", successCount.Load(), float64(successCount.Load())/float64(actualTotal)*100)
	fmt.Printf("Erros:                %d (%.2f%%)\n", errorCount.Load(), float64(errorCount.Load())/float64(actualTotal)*100)
	fmt.Printf("\n--- DISTRIBUIÇÃO DE LATÊNCIA ---\n")
	fmt.Printf("Min:  %v\n", minLat)
	fmt.Printf("Média:%v\n", avgLat)
	fmt.Printf("p50:  %v\n", p50)
	fmt.Printf("p90:  %v\n", p90)
	fmt.Printf("p95:  %v\n", p95)
	fmt.Printf("p99:  %v\n", p99)
	fmt.Printf("Max:  %v\n", maxLat)
	fmt.Printf("============================================================\n")

	if errorCount.Load() > 0 {
		os.Exit(1)
	}
}
