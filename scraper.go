package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	START_ID        = flag.String("start-id", "", "Starting post ID (base36)")
	COUNT           = flag.Int("count", 0, "Number of posts to scrape")
	PROXIES_STRING  = flag.String("proxies", "", "Comma-separated list of proxy URLs (optional)")
	OUTPUT_FILENAME = flag.String("output-file", "scraped_posts.json", "Output JSON file")
)

//go:embed useragents.txt
var USER_AGENT_FILE string

func main() {
	flag.Parse()

	if *START_ID == "" || *COUNT <= 0 {
		flag.Usage()
		os.Exit(1)
	}

	userAgents := strings.Split(strings.ReplaceAll(USER_AGENT_FILE, "\r\n", "\n"), "\n")
	if len(userAgents) == 0 {
		// Just as a failsafe, but we should never hit it
		userAgents = []string{
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
			"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
		}
	}

	var proxies []string
	if *PROXIES_STRING != "" {
		proxies = strings.Split(*PROXIES_STRING, ",")
	}

	// Parse starting ID to integer
	startNum, err := strconv.ParseInt(*START_ID, 36, 64)
	if err != nil {
		log.Fatalf("Invalid starting post ID: %v", err)
	}

	// Channel for tasks (postIDs as string)
	tasks := make(chan string, *COUNT)
	results := make(chan []byte)
	skipped := make(chan string)
	// Sender goroutine
	go func() {
		for i := 0; i < *COUNT; i++ {
			num := startNum + int64(i)
			postID := strconv.FormatInt(num, 36)
			tasks <- postID
		}

		close(tasks)
	}()

	var wg sync.WaitGroup

	// Start 10 workers (for up to 10 in-flight requests)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		proxy := ""
		if len(proxies) != 0 {
			proxy = proxies[rand.Intn(len(proxies))]
		}
		go scrapeWorker(&wg, tasks, skipped, results, proxy, userAgents[rand.Intn(len(userAgents))])
	}

	// Start writer goroutine
	go writeResultsToFile(results, *OUTPUT_FILENAME)
	go writeSkippedToFile(skipped, strings.Split(*OUTPUT_FILENAME, ".")[0]+"_skipped.json")

	wg.Wait()
	close(results)
	close(skipped)

	log.Println("Scraping completed")
}

func scrapeWorker(wg *sync.WaitGroup, tasks <-chan string, skipped chan<- string, results chan<- []byte, proxy, userAgent string) {
	defer wg.Done()

	// Create HTTP transport
	tr := &http.Transport{}
	if proxy != "" {
		// Parse proxy URL
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			log.Printf("Invalid proxy %s: %v", proxy, err)
			return
		}
		tr.Proxy = http.ProxyURL(proxyURL)
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   30 * time.Second,
	}

	consecutive429 := 0

	for postID := range tasks {
		// Build URL
		apiURL := fmt.Sprintf("https://www.reddit.com/api/info.json?id=t3_%s", postID)

		// Create request
		req, err := http.NewRequest("GET", apiURL, nil)
		if err != nil {
			log.Printf("Failed to create request for post %s: %v", postID, err)
			break
		}
		req.Header.Set("User-Agent", userAgent)

		// Execute request
		resp, err := client.Do(req)
		if err != nil {
			skipped <- postID
			log.Printf("Failed to fetch post %s: %v", postID, err)
			break
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			skipped <- postID

			log.Printf("Non-OK status for post %s: %d", postID, resp.StatusCode)
			if resp.StatusCode == http.StatusForbidden { // 403
				log.Printf("403 encountered, shutting down this scraper")
				break
			} else if resp.StatusCode == http.StatusTooManyRequests { // 429
				consecutive429++
				if consecutive429 >= 2 {
					log.Printf("too many 429 on this scraper, shutting down")

					break
				}
				log.Printf("skipped post %s due to 429", postID)
			} else {
				consecutive429 = 0
			}
			time.Sleep(10 * time.Second) // Sleep longer on non-200 to cool down this proxy/userAgent
			continue
		}

		consecutive429 = 0

		// Read response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read response for post %s: %v", postID, err)
			continue
		}

		// Check if body is valid JSON
		if !json.Valid(body) {
			log.Printf("Invalid JSON response for post %s (possibly blocked or HTML)", postID)
			continue
		}

		// Send to results
		results <- body

		log.Printf("Successfully scraped post %s", postID)

		// Rate limit: sleep 1 second between requests
		time.Sleep(time.Second)
	}
	fmt.Println("WORKER DONE")
}

func writeSkippedToFile(skipped <-chan string, outputFile string) {
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()
	for id := range skipped {
		f.WriteString(id + "\n")
	}
}

func writeResultsToFile(results <-chan []byte, outputFile string) {
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()

	_, err = f.Write([]byte("["))
	if err != nil {
		log.Fatalf("Failed to write to output file: %v", err)
	}

	first := true
	for body := range results {
		if !first {
			_, err = f.Write([]byte(","))
			if err != nil {
				log.Fatalf("Failed to write to output file: %v", err)
			}
		}
		first = false

		_, err = f.Write(body)
		if err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}
	}

	_, err = f.Write([]byte("]"))
	if err != nil {
		log.Fatalf("Failed to write to output file: %v", err)
	}

	log.Printf("Successfully wrote to %s", outputFile)
}
