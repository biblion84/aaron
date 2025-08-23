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

// I want this scraper to be a bit more production ready.
// Need to be able to fetch posts infinitely. Rotating proxies when they're blocked
// Retrying IDs that have failed

var (
	START_ID        = flag.String("start-id", "", "Starting post ID (base36)")
	COUNT           = flag.Int("count", 0, "Number of posts to scrape")
	PROXIES_STRING  = flag.String("proxies", "", "Comma-separated list of proxy URLs (optional)")
	OUTPUT_FILENAME = flag.String("output-file", "scraped_posts.json", "Output JSON file")
)

// We can fetch max 100 post per request max
const STEP_SIZE = 100

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

	tasks := make(chan int64, 130)
	results := make(chan []byte)
	skipped := make(chan int64)
	throttle := make(chan bool)
	i := 0
	go func() {
		// Indefinitely feed new IDs
		for range *COUNT {
			select {
			case <-throttle:
				time.Sleep(time.Second)
			default:
				i += STEP_SIZE
				num := startNum + int64(i)
				tasks <- num
			}
		}
	}()

	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		proxy := ""
		if len(proxies) != 0 {
			proxy = proxies[rand.Intn(len(proxies))]
		}
		go scrapeWorker(&wg, tasks, skipped, throttle, results, proxy, userAgents[rand.Intn(len(userAgents))])
	}

	go writeResultsToFile(results, *OUTPUT_FILENAME)
	go writeSkippedToFile(skipped, strings.Split(*OUTPUT_FILENAME, ".")[0]+"_skipped.json")

	wg.Wait()
	close(results)
	close(skipped)

	log.Println("Scraping completed")
}

func scrapeWorker(wg *sync.WaitGroup, tasks <-chan int64, skipped chan<- int64, throttle chan<- bool, results chan<- []byte, proxy, userAgent string) {
	defer wg.Done()

	tr := &http.Transport{}
	if proxy != "" {
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
	//var postResponse PostResponse

	posts := make([]string, STEP_SIZE)
	for postID := range tasks {
		for i := range STEP_SIZE {
			posts[i] = "t3_" + strconv.FormatInt(postID+int64(i), 36)
		}

		apiURL := fmt.Sprintf("https://www.reddit.com/api/info.json?id=%s", strings.Join(posts, ","))

		req, err := http.NewRequest("GET", apiURL, nil)
		if err != nil {
			log.Printf("Failed to create request for post %s: %v", postID, err)
			break
		}
		req.Header.Set("User-Agent", userAgent)

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
			// Sleep longer on non-200 to cool down this proxy/userAgent
			time.Sleep(10 * time.Second)
			continue
		}

		consecutive429 = 0

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to read response for post %s: %v", postID, err)
			continue
		}

		if !json.Valid(body) {
			log.Printf("Invalid JSON response for post %s (possibly blocked or HTML)", postID)
			continue
		}

		//if err := json.Unmarshal(body, &postResponse); err != nil {
		//	log.Printf("error while unmarshalling %s : %v", postID, err)
		//	continue
		//}
		//
		//if len(postResponse.Data.Children) == 0 && postResponse.Kind != "" {
		//	// Throttling as we hit the last post
		//	throttle <- true
		//	continue
		//}

		results <- body

		log.Printf("Successfully scraped post %s", postID)

		time.Sleep(time.Second)
	}
	fmt.Println("WORKER DONE")
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
