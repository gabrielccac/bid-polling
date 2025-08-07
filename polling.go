package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/valyala/fasthttp"
)

type TokenData struct {
	Token     string  `json:"token"`
	Duration  float64 `json:"duration"`
	WorkerID  int     `json:"worker_id"`
	Timestamp float64 `json:"timestamp"`
	Source    string  `json:"source"`
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run polling.go <bearer_token> <interval_ms> <num_workers>")
		fmt.Println("Example: go run polling.go your_token 1000 4")
		os.Exit(1)
	}

	token := os.Args[1]
	intervalMs := os.Args[2]
	numWorkers, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Printf("Invalid number of workers: %v\n", err)
		os.Exit(1)
	}

	interval, err := time.ParseDuration(intervalMs + "ms")
	if err != nil {
		fmt.Printf("Invalid interval: %v\n", err)
		os.Exit(1)
	}

	// Fixed endpoint URL with captcha token placeholder
	urlPattern := "https://cnetmobile.estaleiro.serpro.gov.br/comprasnet-disputa/v1/compras/92999206900372025/itens/1/lances/por-participante?captcha=%s&tamanhoPagina=20&pagina=0"

	// Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		fmt.Printf("❌ Redis connection failed: %v\n", err)
		fmt.Println("Cannot continue without Redis for token queue")
		os.Exit(1)
	}
	fmt.Println("✅ Redis connection successful")

	// Global atomic counters  
	var globalSuccessCounter int64
	var globalRequestCounter int64
	var lastPollTime int64 // For coverage monitoring
	
	// Coverage quality tracking
	var totalGaps int64
	var gapSum int64
	var minGap int64 = 999999
	var maxGap int64 = 0

	// Get token from Redis queue
	getTokenFromQueue := func(ctx context.Context) (*TokenData, error) {
		// Pop token from Redis list (blocking pop with 1 second timeout)
		result, err := redisClient.BLPop(ctx, 1*time.Second, "rest_token_queue").Result()
		if err != nil {
			return nil, err
		}

		if len(result) < 2 {
			return nil, fmt.Errorf("invalid queue response")
		}

		// result[1] contains the token key like "rest_token:8KRkukrJmM"
		tokenKey := result[1]

		// Get the actual token data from Redis
		tokenDataStr, err := redisClient.Get(ctx, tokenKey).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get token data for key %s: %v", tokenKey, err)
		}

		var tokenData TokenData
		if err := json.Unmarshal([]byte(tokenDataStr), &tokenData); err != nil {
			return nil, fmt.Errorf("failed to parse token data: %v", err)
		}

		// Delete the token key since we've consumed it
		redisClient.Del(ctx, tokenKey)

		return &tokenData, nil
	}

	// Simplified stats tracking for continuous polling
	type PollingStats struct {
		mu           sync.RWMutex
		count        int
		totalRTT     time.Duration
		avgRTT       time.Duration
		minRTT       time.Duration
		maxRTT       time.Duration
		status200    int
		status429    int
		statusOther  int
		statusCounts map[int]int
		startTime    time.Time
	}
	stats := &PollingStats{
		minRTT:       time.Hour,
		statusCounts: make(map[int]int),
		startTime:    time.Now(),
	}

	// Thread-safe methods for PollingStats
	updateStats := func(rtt time.Duration, statusCode int) {
		stats.mu.Lock()
		defer stats.mu.Unlock()

		stats.count++
		stats.totalRTT += rtt
		stats.avgRTT = stats.totalRTT / time.Duration(stats.count)
		if rtt < stats.minRTT {
			stats.minRTT = rtt
		}
		if rtt > stats.maxRTT {
			stats.maxRTT = rtt
		}

		// Track status codes
		stats.statusCounts[statusCode]++
		switch statusCode {
		case 200:
			stats.status200++
		case 429:
			stats.status429++
		default:
			stats.statusOther++
		}
	}

	getStats := func() PollingStats {
		stats.mu.RLock()
		defer stats.mu.RUnlock()
		statusCountsCopy := make(map[int]int)
		for k, v := range stats.statusCounts {
			statusCountsCopy[k] = v
		}
		return PollingStats{
			count:        stats.count,
			totalRTT:     stats.totalRTT,
			avgRTT:       stats.avgRTT,
			minRTT:       stats.minRTT,
			maxRTT:       stats.maxRTT,
			status200:    stats.status200,
			status429:    stats.status429,
			statusOther:  stats.statusOther,
			statusCounts: statusCountsCopy,
			startTime:    stats.startTime,
		}
	}

	// Worker function for continuous polling with staggered start
	worker := func(workerID int, wg *sync.WaitGroup, stopChan <-chan struct{}) {
		defer wg.Done()

		// Calculate stagger delay to distribute workers evenly
		staggerDelay := interval / time.Duration(numWorkers)
		workerStartDelay := time.Duration(workerID-1) * staggerDelay

		// Add initial stagger delay before starting polling
		fmt.Printf("Worker %d starting with %v delay for staggered coverage\n", workerID, workerStartDelay)
		time.Sleep(workerStartDelay)

		client := &fasthttp.Client{
			MaxConnsPerHost:     10,
			MaxIdleConnDuration: 30 * time.Second,
		}

		requestCount := 0

		for {
			select {
			case <-stopChan:
				fmt.Printf("Worker %d stopping after %d requests\n", workerID, requestCount)
				return
			default:
				requestCount++

				// Get captcha token from queue
				tokenData, tokenErr := getTokenFromQueue(ctx)
				if tokenErr != nil {
					fmt.Printf("Worker %d Request #%d failed to get token: %v\n", workerID, requestCount, tokenErr)
					time.Sleep(interval)
					continue
				}

				// Build URL with captcha token
				url := fmt.Sprintf(urlPattern, tokenData.Token)

				req := fasthttp.AcquireRequest()
				resp := fasthttp.AcquireResponse()

				req.SetRequestURI(url)
				req.Header.SetMethod("GET")
				req.Header.Set("Authorization", "Bearer "+token)

				start := time.Now()
				err := client.Do(req, resp)
				rtt := time.Since(start)

				globalRequestSeq := atomic.AddInt64(&globalRequestCounter, 1)

				// Coverage monitoring - track intervals between polls
				currentTime := time.Now().UnixMilli()
				prevTime := atomic.SwapInt64(&lastPollTime, currentTime)
				var intervalGap int64 = 0
				if prevTime > 0 {
					intervalGap = currentTime - prevTime
					
					// Update coverage statistics
					atomic.AddInt64(&totalGaps, 1)
					atomic.AddInt64(&gapSum, intervalGap)
					
					// Update min gap
					for {
						currentMin := atomic.LoadInt64(&minGap)
						if intervalGap >= currentMin || atomic.CompareAndSwapInt64(&minGap, currentMin, intervalGap) {
							break
						}
					}
					
					// Update max gap
					for {
						currentMax := atomic.LoadInt64(&maxGap)
						if intervalGap <= currentMax || atomic.CompareAndSwapInt64(&maxGap, currentMax, intervalGap) {
							break
						}
					}
				}

				tokenAge := time.Now().Unix() - int64(tokenData.Timestamp)

				if err != nil {
					fmt.Printf("Worker %d Request #%d [Global #%d] failed: %v (token age: %ds)\n", workerID, requestCount, globalRequestSeq, err, tokenAge)
					updateStats(rtt, 0)
				} else {
					statusCode := resp.StatusCode()
					updateStats(rtt, statusCode)

					if statusCode == 200 {
						atomic.AddInt64(&globalSuccessCounter, 1)
					}

					// Parse response for bid information
					responseBody := string(resp.Body())
					// bidInfo := ""
					if statusCode == 200 && len(responseBody) > 0 {
						// Try to extract some bid info from response
						if strings.Contains(responseBody, "valorInformado") {
							// bidInfo = " - Contains bid data"
						} else {
							// bidInfo = " - No bids found"
						}
					}

					// Format timestamp as HH:MM:SS:MS (milliseconds as 4 digits)
					timestamp := time.Now().Format("15:04:05.0000")
					timestamp = strings.Replace(timestamp, ".", ":", 1) // Turn HH:MM:SS.1234 into HH:MM:SS:1234

					// Show interval gap for coverage monitoring
					gapInfo := ""
					if intervalGap > 0 {
						gapInfo = fmt.Sprintf(" [gap: %dms]", intervalGap)
					}

					fmt.Printf("Worker %d Request #%d [Global #%d] - Status: %d @ %s%s\n",
						workerID, requestCount, globalRequestSeq, statusCode, timestamp, gapInfo)

				}

				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)

				// Sleep for interval
				time.Sleep(interval)
			}
		}
	}

	// Coverage reporting goroutine
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				gaps := atomic.LoadInt64(&totalGaps)
				if gaps > 0 {
					sum := atomic.LoadInt64(&gapSum)
					min := atomic.LoadInt64(&minGap)
					max := atomic.LoadInt64(&maxGap)
					avg := sum / gaps
					
					requests := atomic.LoadInt64(&globalRequestCounter)
					successes := atomic.LoadInt64(&globalSuccessCounter)
					
					fmt.Printf("\n=== COVERAGE STATS ===\n")
					fmt.Printf("Polling gaps: Avg: %dms | Min: %dms | Max: %dms\n", avg, min, max)
					fmt.Printf("Coverage quality: %d gaps measured | Success rate: %.1f%%\n", 
						gaps, float64(successes)/float64(requests)*100)
					
					// Calculate coverage consistency
					expectedGap := int64(interval.Milliseconds()) / int64(numWorkers)
					consistency := 100.0 - (float64(abs(avg-expectedGap))/float64(expectedGap))*100
					fmt.Printf("Expected gap: %dms | Consistency: %.1f%%\n", expectedGap, consistency)
					fmt.Println()
				}
			}
		}
	}()

	staggerDelay := interval / time.Duration(numWorkers)
	effectiveRate := time.Second / staggerDelay

	fmt.Printf("Starting staggered polling with %d workers at %v intervals\n", numWorkers, interval)
	fmt.Printf("Stagger delay: %v per worker (effective rate: %.1f polls/sec)\n", staggerDelay, float64(effectiveRate))
	fmt.Printf("Endpoint: %s\n", strings.Replace(urlPattern, "%s", "{token}", 1))
	fmt.Println("Press Ctrl+C to stop")

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i+1, &wg, stopChan)
	}

	// Wait for all workers to complete (will run indefinitely until Ctrl+C)
	wg.Wait()

	// Final coverage statistics
	finalStats := getStats()
	finalGaps := atomic.LoadInt64(&totalGaps)
	finalRequests := atomic.LoadInt64(&globalRequestCounter)
	finalSuccesses := atomic.LoadInt64(&globalSuccessCounter)
	elapsed := time.Since(finalStats.startTime)

	fmt.Printf("\n=== FINAL COVERAGE STATISTICS ===\n")
	fmt.Printf("Total runtime: %v\n", elapsed.Round(time.Second))
	fmt.Printf("Total requests: %d | Successful: %d (%.1f%%)\n", 
		finalRequests, finalSuccesses, float64(finalSuccesses)/float64(finalRequests)*100)
	
	if finalGaps > 0 {
		finalSum := atomic.LoadInt64(&gapSum)
		finalMin := atomic.LoadInt64(&minGap)
		finalMax := atomic.LoadInt64(&maxGap)
		finalAvg := finalSum / finalGaps
		
		expectedGap := int64(interval.Milliseconds()) / int64(numWorkers)
		consistency := 100.0 - (float64(abs(finalAvg-expectedGap))/float64(expectedGap))*100
		
		fmt.Printf("\n=== COVERAGE QUALITY ===\n")
		fmt.Printf("Polling gaps measured: %d\n", finalGaps)
		fmt.Printf("Gap distribution: Avg: %dms | Min: %dms | Max: %dms\n", finalAvg, finalMin, finalMax)
		fmt.Printf("Expected gap: %dms | Achieved consistency: %.1f%%\n", expectedGap, consistency)
		fmt.Printf("Effective polling rate: %.2f polls/sec\n", float64(finalRequests)/elapsed.Seconds())
		
		// Coverage effectiveness
		if consistency >= 90 {
			fmt.Printf("✅ Excellent coverage - staggering working perfectly\n")
		} else if consistency >= 75 {
			fmt.Printf("⚠️  Good coverage - minor timing variations\n")
		} else {
			fmt.Printf("❌ Poor coverage - significant clustering detected\n")
		}
	}
}
