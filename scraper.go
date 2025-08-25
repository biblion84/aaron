package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// I want this scraper to be a bit more production ready.
// Need to be able to fetch posts infinitely. Rotating proxies when they're blocked
// Retrying IDs that have failed

var (
	START_ID        = flag.String("start-id", "", "Starting post ID (base36)")
	PROXIES_STRING  = flag.String("proxies", "", "Comma-separated list of proxy URLs (optional)")
	OUTPUT_FILENAME = flag.String("output-file", "scraped_posts.json", "Output JSON file")
)

// We can fetch max 100 post per request max
const STEP_SIZE = 100
const WORKERS = 10

var (
	ERR_NON_200_RESPONSE = errors.New("non 200 response")
)

//go:embed useragents.txt
var USER_AGENT_FILE string

func main() {
	flag.Parse()

	if *START_ID == "" {
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

	tasks := make(chan int64, WORKERS)
	results := make(chan []byte)
	skipped := make(chan int64, WORKERS*2)
	inflight := make(chan struct{}, WORKERS)
	i := int64(0)

	shutdown := false
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal")
		shutdown = true
	}()

	go func() {
		// Indefinitely feed new IDs
		// Retrying IDs from the skipped channel first
		for {
			if shutdown {
				close(tasks)
				return
			}
			inflight <- struct{}{}
			var nextId int64
			if len(skipped) > 0 {
				nextId = <-skipped
			} else {
				i += STEP_SIZE
				nextId = startNum + i
			}
			tasks <- nextId
		}
	}()

	var wg sync.WaitGroup

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go scrapeWorker(&wg, tasks, skipped, results, inflight, proxies, userAgents)
	}

	go writeResultsToFile(results, *OUTPUT_FILENAME)
	go writeSkippedToFile(skipped, strings.Split(*OUTPUT_FILENAME, ".")[0]+"_skipped.json")

	wg.Wait()
	close(results)
	close(skipped)
	close(inflight)

	log.Println("Scraping completed")
}

type Worker struct {
	proxy     string
	userAgent string
	client    http.Client
}

func (w *Worker) SetRandomIdentity(proxies, userAgents []string) {
	w.proxy = proxies[rand.Intn(len(proxies))]
	w.userAgent = userAgents[rand.Intn(len(userAgents))]

	w.client = http.Client{
		Timeout: time.Second * 30,
	}

	proxyURL, err := url.Parse(w.proxy)
	if err != nil {
		log.Printf("Invalid proxy %s: %v", w.proxy, err)
		return
	}

	w.client.Transport = &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}
}

func scrapeWorker(wg *sync.WaitGroup, tasks <-chan int64, skipped chan<- int64, results chan<- []byte, inflight <-chan struct{}, proxies, userAgents []string) {
	defer wg.Done()

	worker := Worker{}
	worker.SetRandomIdentity(proxies, userAgents)

	consecutiveNonValidResponse := 0

	posts := make([]string, STEP_SIZE)
	for postID := range tasks {

		for i := range STEP_SIZE {
			posts[i] = "t3_" + strconv.FormatInt(postID+int64(i), 36)
		}

		apiURL := fmt.Sprintf("https://www.reddit.com/api/info.json?id=%s", strings.Join(posts, ","))

		response, err := worker.DoRequest(apiURL)
		if err != nil {
			if errors.Is(err, ERR_NON_200_RESPONSE) && consecutiveNonValidResponse < 1 {
				consecutiveNonValidResponse++
			} else {
				log.Printf("re-rolling worker identity")
				worker.SetRandomIdentity(proxies, userAgents)
			}
			log.Printf("%d skipped, will retry", postID)
			skipped <- postID
			<-inflight
			continue
		} else {
			consecutiveNonValidResponse = 0
		}

		<-inflight

		results <- response

		log.Printf("Successfully scraped post %d", postID)

		time.Sleep(time.Second)
	}
	fmt.Println("WORKER DONE")
}

func (w *Worker) DoRequest(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for post: %v", err)
	}
	req.Header.Set("User-Agent", w.userAgent)

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Invalid status code for post: %v, sleeping to cool off", resp.StatusCode)
		time.Sleep(3 * time.Second)
		return nil, ERR_NON_200_RESPONSE
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response for post: %v", err)
	}

	if !json.Valid(body) {
		return nil, fmt.Errorf("invalid JSON response")
	}

	return body, nil
}

func writeSkippedToFile(skipped <-chan int64, outputFile string) {
	f, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()
	for id := range skipped {
		f.WriteString(strconv.Itoa(int(id)) + "\n")
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
