package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rix4uni/commoncrawlscans/banner"
	"github.com/spf13/pflag"
)

const (
	configDirName = ".config/commoncrawlscans"
)

const (
	baseURL = "https://data.commoncrawl.org"
)

type config struct {
	files     int
	output    string
	retries   int
	resume    bool
	exclude   string
	include   string
	listCrawl bool
	silent    bool
	version   bool
	skipDedup bool
}

type urlResult struct {
	path string
	err  error
}

func main() {
	cfg := parseFlags()

	// Print version and exit if -version flag is provided
	if cfg.version {
		banner.PrintBanner()
		banner.PrintVersion()
		return
	}

	// List crawl versions and exit if --list-crawl flag is provided
	if cfg.listCrawl {
		crawls, err := fetchCrawlList()
		if err != nil {
			log.Fatalf("Failed to fetch crawl list: %v", err)
		}
		for _, crawl := range crawls {
			fmt.Println(crawl)
		}
		return
	}

	// Print banner if not in silent mode
	if !cfg.silent {
		banner.PrintBanner()
	}

	// Read crawl version from stdin
	crawlVersion, err := readCrawlVersion()
	if err != nil {
		log.Fatalf("Failed to read crawl version from stdin: %v", err)
	}
	crawlVersion = strings.TrimSpace(crawlVersion)
	if crawlVersion == "" {
		log.Fatalf("Crawl version cannot be empty")
	}

	log.Printf("Using crawl version: %s", crawlVersion)

	// Fetch index paths
	log.Println("Fetching index paths...")
	paths, err := fetchIndexPaths(crawlVersion, cfg.retries)
	if err != nil {
		log.Fatalf("Failed to fetch index paths: %v", err)
	}

	log.Printf("Found %d .gz files to process", len(paths))

	// Setup resume functionality
	configDir, err := getConfigDir()
	if err != nil {
		log.Fatalf("Failed to get config directory: %v", err)
	}

	resumeFile := filepath.Join(configDir, fmt.Sprintf("%s.resume", crawlVersion))
	failedFile := filepath.Join(configDir, fmt.Sprintf("%s.failed", crawlVersion))

	// Filter paths if resuming
	if cfg.resume {
		processedPaths, err := readResumeFile(resumeFile)
		if err != nil && !os.IsNotExist(err) {
			log.Fatalf("Failed to read resume file: %v", err)
		}
		if len(processedPaths) > 0 {
			processedSet := make(map[string]bool)
			for _, p := range processedPaths {
				processedSet[p] = true
			}
			var filteredPaths []string
			for _, path := range paths {
				if !processedSet[path] {
					filteredPaths = append(filteredPaths, path)
				}
			}
			log.Printf("Resuming: %d files already processed, %d files remaining", len(processedPaths), len(filteredPaths))
			paths = filteredPaths
		} else {
			log.Println("Resume file not found or empty, starting from beginning")
		}
	}

	// Setup output - always write to file, never stdout
	outputDir := cfg.output
	if outputDir == "" {
		outputDir = crawlVersion // Default directory is the crawl version
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Parse include/exclude lists
	includeSet := parseIncludeList(cfg.include)
	excludeSet := parseExcludeList(cfg.exclude)
	usingInclude := cfg.include != ""

	// Create main output file only if not using --include flag
	var outputFile *os.File
	if !usingInclude {
		outputPath := filepath.Join(outputDir, "commoncrawlscans.txt")
		var err error
		outputFile, err = os.Create(outputPath)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outputFile.Close()
		log.Printf("Writing output to %s", outputPath)
	}

	// Define all file extensions to match
	extensions := []string{
		".php", ".aspx", ".asp", ".jsp", ".jspx", ".do", ".sql", ".log",
		".py", ".java", ".conf", ".cnf", ".ini", ".env", ".sh", ".bak",
		".backup", ".tar", ".yml", ".swp", ".old", ".svn", ".htpasswd",
		".htaccess", ".json", ".txt", ".pdf", ".xml", ".xls", ".xlsx",
		".ppt", ".pptx", ".doc", ".docx", ".csv", ".db", ".js", ".zip",
	}

	// Create file writers for each extension
	extensionWriters := make(map[string]io.Writer)
	extensionFiles := make(map[string]*os.File)
	for _, ext := range extensions {
		// Remove leading dot for filename
		extName := ext[1:] // Remove the dot

		// Check if this extension should be created based on include/exclude logic
		if usingInclude {
			// Include mode: only create if in include list
			if !includeSet[extName] {
				continue
			}
		} else {
			// Exclude mode: create if not in exclude list
			if excludeSet[extName] {
				continue
			}
		}

		extPath := filepath.Join(outputDir, extName+".txt")
		extFile, err := os.Create(extPath)
		if err != nil {
			log.Fatalf("Failed to create %s file: %v", extName, err)
		}
		extensionFiles[ext] = extFile
		extensionWriters[ext] = extFile
		log.Printf("Writing %s filenames to %s", extName, extPath)
	}

	// Ensure all extension files are closed on exit
	defer func() {
		for _, f := range extensionFiles {
			f.Close()
		}
	}()

	// Create subdomains.txt file based on include/exclude logic
	var subdomainsFile *os.File
	shouldCreateSubdomains := false
	if usingInclude {
		shouldCreateSubdomains = includeSet["subdomains"]
	} else {
		shouldCreateSubdomains = !excludeSet["subdomains"]
	}

	if shouldCreateSubdomains {
		subdomainsPath := filepath.Join(outputDir, "subdomains.txt")
		var err error
		subdomainsFile, err = os.Create(subdomainsPath)
		if err != nil {
			log.Fatalf("Failed to create subdomains file: %v", err)
		}
		defer subdomainsFile.Close()
		log.Printf("Writing subdomains to %s", subdomainsPath)
	}

	// Create ips.txt file based on include/exclude logic
	var ipsFile *os.File
	shouldCreateIPs := false
	if usingInclude {
		shouldCreateIPs = includeSet["ips"]
	} else {
		shouldCreateIPs = !excludeSet["ips"]
	}

	if shouldCreateIPs {
		ipsPath := filepath.Join(outputDir, "ips.txt")
		var err error
		ipsFile, err = os.Create(ipsPath)
		if err != nil {
			log.Fatalf("Failed to create ips file: %v", err)
		}
		defer ipsFile.Close()
		log.Printf("Writing IPs to %s", ipsPath)
	}

	// Create HTTP client with connection pooling
	// Very long timeout for file downloads - files can be very large (hundreds of MBs)
	client := &http.Client{
		Timeout: 2 * time.Hour, // Allow large file downloads to complete
		Transport: &http.Transport{
			MaxIdleConns:          cfg.files * 2,
			MaxIdleConnsPerHost:   cfg.files,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second, // Timeout for initial response headers
		},
	}

	// Process files concurrently
	processFiles(client, paths, cfg.files, cfg.retries, outputFile, extensionWriters, subdomainsFile, ipsFile, resumeFile, failedFile, excludeSet, cfg.skipDedup)

	log.Println("Processing complete")
}

func parseFlags() *config {
	cfg := &config{}
	pflag.IntVar(&cfg.files, "files", 1, "Number of files to process concurrently")
	pflag.StringVar(&cfg.output, "output", "", "Directory name to save output file (default: crawl version)")
	pflag.IntVar(&cfg.retries, "retries", 3, "Number of retry attempts for failed requests")
	pflag.BoolVar(&cfg.resume, "resume", false, "Resume from previous run using resume file")
	pflag.StringVar(&cfg.exclude, "exclude", "", "Comma-separated list of file types to exclude (e.g., \"subdomains,php,zip\")")
	pflag.StringVar(&cfg.include, "include", "", "Comma-separated list of file types to include (e.g., \"subdomains,ips\")")
	pflag.BoolVar(&cfg.listCrawl, "list-crawl", false, "List all available Common Crawl scans and exit.")
	pflag.BoolVar(&cfg.silent, "silent", false, "Silent mode.")
	pflag.BoolVar(&cfg.version, "version", false, "Print the version of the tool and exit.")
	pflag.BoolVar(&cfg.skipDedup, "skip-dedup", false, "Skip deduplication to reduce RAM usage. Output may contain duplicates. Use 'sort -u' to deduplicate later.")
	pflag.Parse()

	// Validate that both --include and --exclude are not used together
	if cfg.include != "" && cfg.exclude != "" {
		log.Fatalf("Error: Cannot use both --include and --exclude flags together")
	}

	return cfg
}

// parseExcludeList parses a comma-separated exclude string into a map for fast lookup
// Returns a map[string]bool where keys are normalized (lowercase, trimmed)
func parseExcludeList(excludeStr string) map[string]bool {
	excludeSet := make(map[string]bool)
	if excludeStr == "" {
		return excludeSet
	}

	// Split by comma and process each item
	items := strings.Split(excludeStr, ",")
	for _, item := range items {
		// Trim spaces and convert to lowercase
		normalized := strings.ToLower(strings.TrimSpace(item))
		if normalized != "" {
			excludeSet[normalized] = true
		}
	}

	return excludeSet
}

// parseIncludeList parses a comma-separated include string into a map for fast lookup
// Returns a map[string]bool where keys are normalized (lowercase, trimmed)
func parseIncludeList(includeStr string) map[string]bool {
	includeSet := make(map[string]bool)
	if includeStr == "" {
		return includeSet
	}

	// Split by comma and process each item
	items := strings.Split(includeStr, ",")
	for _, item := range items {
		// Trim spaces and convert to lowercase
		normalized := strings.ToLower(strings.TrimSpace(item))
		if normalized != "" {
			includeSet[normalized] = true
		}
	}

	return includeSet
}

func readCrawlVersion() (string, error) {
	// Check if stdin is a terminal (not a pipe)
	// If it's a terminal, user didn't pipe input, so fetch from web
	if isStdinTerminal() {
		log.Println("No input provided, fetching latest crawl version from Common Crawl...")
		return fetchLatestCrawlVersion()
	}

	// Stdin is a pipe, try to read from it
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		// No input provided in pipe, fetch latest crawl version from web
		log.Println("No input provided, fetching latest crawl version from Common Crawl...")
		return fetchLatestCrawlVersion()
	}
	version := scanner.Text()
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading from stdin: %w", err)
	}
	return version, nil
}

func isStdinTerminal() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		// If we can't stat stdin, assume it's not a terminal and try to read
		return false
	}
	// Check if stdin is a character device (terminal)
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func fetchLatestCrawlVersion() (string, error) {
	url := "https://commoncrawl.org/latest-crawl"

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	// Make HTTP GET request
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch latest crawl version: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse HTML to extract crawl version from <p class="paragraph-30">...</p>
	htmlContent := string(body)
	startPattern := `<p class="paragraph-30">`
	startIdx := strings.Index(htmlContent, startPattern)
	if startIdx == -1 {
		return "", fmt.Errorf("could not find crawl version in HTML response")
	}

	// Move past the opening tag
	startIdx += len(startPattern)

	// Find the closing </p> tag
	endIdx := strings.Index(htmlContent[startIdx:], "</p>")
	if endIdx == -1 {
		return "", fmt.Errorf("could not find closing tag for crawl version")
	}

	// Extract the crawl version
	crawlVersion := strings.TrimSpace(htmlContent[startIdx : startIdx+endIdx])
	if crawlVersion == "" {
		return "", fmt.Errorf("extracted crawl version is empty")
	}

	log.Printf("Fetched latest crawl version: %s", crawlVersion)
	return crawlVersion, nil
}

func fetchCrawlList() ([]string, error) {
	url := "https://index.commoncrawl.org/collinfo.json"

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	// Make HTTP GET request
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch crawl list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Parse JSON response
	var crawls []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&crawls); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Extract IDs
	var ids []string
	for _, crawl := range crawls {
		if id, ok := crawl["id"].(string); ok {
			ids = append(ids, id)
		}
	}

	return ids, nil
}

func getConfigDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	configDir := filepath.Join(homeDir, configDirName)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create config directory: %w", err)
	}
	return configDir, nil
}

func readResumeFile(resumeFile string) ([]string, error) {
	file, err := os.Open(resumeFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var paths []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			paths = append(paths, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading resume file: %w", err)
	}

	return paths, nil
}

func fetchIndexPaths(crawlVersion string, retries int) ([]string, error) {
	indexPathsURL := fmt.Sprintf("https://data.commoncrawl.org/crawl-data/%s/cc-index.paths.gz", crawlVersion)
	resp, err := retryHTTP(indexPathsURL, retries)
	if err != nil {
		return nil, fmt.Errorf("failed to download index paths: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Decompress gzip
	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Read and filter for .gz files
	var paths []string
	scanner := bufio.NewScanner(gzReader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasSuffix(line, ".gz") {
			paths = append(paths, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading index paths: %w", err)
	}

	return paths, nil
}

func processFiles(client *http.Client, paths []string, numWorkers int, retries int, outputWriter io.Writer, extensionWriters map[string]io.Writer, subdomainsWriter, ipsWriter io.Writer, resumeFile, failedFile string, excludeSet map[string]bool, skipDedup bool) {
	// Create channels for work distribution and results
	pathChan := make(chan string, numWorkers*2)
	resultChan := make(chan urlResult, numWorkers*2)
	urlChan := make(chan []byte, numWorkers*100) // Buffer for URL batches

	// Open resume and failed files for appending
	resumeFileHandle, err := os.OpenFile(resumeFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open resume file: %v", err)
	}
	defer resumeFileHandle.Close()

	failedFileHandle, err := os.OpenFile(failedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open failed file: %v", err)
	}
	defer failedFileHandle.Close()

	resumeWriter := bufio.NewWriterSize(resumeFileHandle, 64*1024)
	failedWriter := bufio.NewWriterSize(failedFileHandle, 64*1024)
	defer resumeWriter.Flush()
	defer failedWriter.Flush()

	// Create shared writer for output file with large buffer (only if outputWriter is not nil)
	var writer *bufio.Writer
	if outputWriter != nil {
		writer = bufio.NewWriterSize(outputWriter, 1024*1024) // 1MB buffer
		defer writer.Flush()
	}

	// Create buffered writers for each extension file
	extensionBuffers := make(map[string]*bufio.Writer)
	var extensionMutex sync.Mutex

	for ext, writer := range extensionWriters {
		extensionBuffers[ext] = bufio.NewWriterSize(writer, 64*1024) // 64KB buffer
	}

	// Deduplication maps for extension files (extension -> filename -> seen)
	// Only initialize if deduplication is enabled
	var seenFilenames map[string]map[string]bool
	if !skipDedup {
		seenFilenames = make(map[string]map[string]bool)
		for ext := range extensionWriters {
			seenFilenames[ext] = make(map[string]bool)
		}
	}

	// Create buffered writers for subdomains and IPs (only if not excluded)
	var subdomainsBuffer *bufio.Writer
	var ipsBuffer *bufio.Writer
	if subdomainsWriter != nil {
		subdomainsBuffer = bufio.NewWriterSize(subdomainsWriter, 64*1024)
	}
	if ipsWriter != nil {
		ipsBuffer = bufio.NewWriterSize(ipsWriter, 64*1024)
	}

	// Deduplication maps for subdomains and IPs
	// Only initialize if deduplication is enabled
	var seenSubdomains map[string]bool
	var seenIPs map[string]bool
	if !skipDedup {
		seenSubdomains = make(map[string]bool)
		seenIPs = make(map[string]bool)
	}
	var subdomainIPMutex sync.Mutex

	// Start dedicated URL writer goroutine (reduces mutex contention)
	var urlWriterWg sync.WaitGroup
	urlWriterWg.Add(1)
	go func() {
		defer urlWriterWg.Done()
		defer func() {
			// Ensure all extension files are flushed on exit
			extensionMutex.Lock()
			for _, bufWriter := range extensionBuffers {
				bufWriter.Flush()
			}
			extensionMutex.Unlock()
			// Flush subdomains and IPs (if not excluded)
			subdomainIPMutex.Lock()
			if subdomainsBuffer != nil {
				subdomainsBuffer.Flush()
			}
			if ipsBuffer != nil {
				ipsBuffer.Flush()
			}
			subdomainIPMutex.Unlock()
		}()

		batchCount := 0
		for urls := range urlChan {
			// Write all URLs to main output (only if writer is not nil)
			if writer != nil {
				writer.Write(urls)
			}

			// Filter and extract filenames for all extensions in one pass
			lines := bytes.Split(urls, []byte{'\n'})
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				// Check all extensions in one efficient pass
				if matchedExt, filename := extractFilenameByExtension(line); matchedExt != "" {
					filenameStr := string(filename)
					extensionMutex.Lock()
					if bufWriter, ok := extensionBuffers[matchedExt]; ok {
						// Check if filename has been seen before (only if deduplication is enabled)
						if skipDedup {
							bufWriter.Write(filename)
							bufWriter.WriteByte('\n')
						} else {
							if !seenFilenames[matchedExt][filenameStr] {
								seenFilenames[matchedExt][filenameStr] = true
								bufWriter.Write(filename)
								bufWriter.WriteByte('\n')
							}
						}
					}
					extensionMutex.Unlock()
				}

				// Extract hostname and check if it's an IP or subdomain
				hostname := extractHostnameFromURL(line)
				if hostname != "" {
					// Check if hostname is an IP address
					if ip := net.ParseIP(hostname); ip != nil {
						// It's an IP address (only process if not excluded)
						if ipsBuffer != nil {
							subdomainIPMutex.Lock()
							if skipDedup {
								ipsBuffer.WriteString(hostname)
								ipsBuffer.WriteByte('\n')
							} else {
								if !seenIPs[hostname] {
									seenIPs[hostname] = true
									ipsBuffer.WriteString(hostname)
									ipsBuffer.WriteByte('\n')
								}
							}
							subdomainIPMutex.Unlock()
						}
					} else {
						// It's a domain/subdomain - validate before writing (only if not excluded)
						if subdomainsBuffer != nil && isValidDomain(hostname) {
							subdomainIPMutex.Lock()
							if skipDedup {
								subdomainsBuffer.WriteString(hostname)
								subdomainsBuffer.WriteByte('\n')
							} else {
								if !seenSubdomains[hostname] {
									seenSubdomains[hostname] = true
									subdomainsBuffer.WriteString(hostname)
									subdomainsBuffer.WriteByte('\n')
								}
							}
							subdomainIPMutex.Unlock()
						}
					}
				}
			}

			batchCount++
			// Flush every 100 batches to ensure data is written
			if batchCount%100 == 0 {
				if writer != nil {
					writer.Flush()
				}
				extensionMutex.Lock()
				for _, bufWriter := range extensionBuffers {
					bufWriter.Flush()
				}
				extensionMutex.Unlock()
				subdomainIPMutex.Lock()
				if subdomainsBuffer != nil {
					subdomainsBuffer.Flush()
				}
				if ipsBuffer != nil {
					ipsBuffer.Flush()
				}
				subdomainIPMutex.Unlock()
			}
		}
		if writer != nil {
			writer.Flush() // Final flush
		}
		extensionMutex.Lock()
		for _, bufWriter := range extensionBuffers {
			bufWriter.Flush()
		}
		extensionMutex.Unlock()
		subdomainIPMutex.Lock()
		if subdomainsBuffer != nil {
			subdomainsBuffer.Flush()
		}
		if ipsBuffer != nil {
			ipsBuffer.Flush()
		}
		subdomainIPMutex.Unlock()
	}()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case path, ok := <-pathChan:
					if !ok {
						return // Channel closed
					}
					select {
					case <-ctx.Done():
						// Context cancelled, exit immediately
						return
					default:
						log.Printf("Processing file: %s", path)
						err := processFile(ctx, client, path, retries, urlChan)
						select {
						case resultChan <- urlResult{path: path, err: err}:
						case <-ctx.Done():
							return
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start result writer goroutine (handles resume/failed file updates)
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	processed := 0

	go func() {
		defer writerWg.Done()
		defer func() {
			// Ensure resume file is flushed on exit (including interrupt)
			resumeWriter.Flush()
			failedWriter.Flush()
		}()

		for i := 0; i < len(paths); i++ {
			result := <-resultChan
			processed++

			if result.err != nil {
				log.Printf("Error processing file %s: %v", result.path, result.err)
				// Write to failed file
				failedWriter.WriteString(result.path + "\n")
				failedWriter.Flush() // Flush immediately to ensure it's saved
			} else {
				// Write to resume file
				resumeWriter.WriteString(result.path + "\n")
				resumeWriter.Flush() // Flush immediately to ensure it's saved
			}

			if processed%10 == 0 {
				log.Printf("Progress: %d/%d files processed", processed, len(paths))
			}
		}
	}()

	// Use sync.Once to ensure channels are only closed once
	var pathChanOnce sync.Once
	var resultChanOnce sync.Once
	var urlChanOnce sync.Once

	closePathChan := func() {
		pathChanOnce.Do(func() {
			close(pathChan)
		})
	}
	closeResultChan := func() {
		resultChanOnce.Do(func() {
			close(resultChan)
		})
	}
	closeUrlChan := func() {
		urlChanOnce.Do(func() {
			close(urlChan)
		})
	}

	// Send work to workers in background
	go func() {
		for _, path := range paths {
			select {
			case pathChan <- path:
			case <-ctx.Done():
				closePathChan()
				return
			}
		}
		closePathChan()
	}()

	// Wait for completion or signal
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		closeResultChan()
		closeUrlChan()
		urlWriterWg.Wait()
		writerWg.Wait()
		done <- true
	}()

	select {
	case <-sigChan:
		log.Println("\nReceived interrupt signal, initiating graceful shutdown...")
		cancel()        // Cancel context to stop all workers
		closePathChan() // Stop assigning new work

		// Give workers a short time to finish, then force exit
		shutdownTimeout := 5 * time.Second
		shutdownDone := make(chan bool, 1)
		go func() {
			wg.Wait()
			closeResultChan()
			closeUrlChan()
			urlWriterWg.Wait()
			writerWg.Wait()
			shutdownDone <- true
		}()

		select {
		case <-shutdownDone:
			log.Println("Graceful shutdown complete. Resume file has been saved.")
		case <-time.After(shutdownTimeout):
			log.Println("Shutdown timeout reached, forcing exit...")
			os.Exit(1)
		}
	case <-done:
		log.Println("All files processed successfully")
	}
}

func processFile(ctx context.Context, client *http.Client, path string, retries int, urlChan chan<- []byte) error {
	// Check if context is cancelled before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	url := baseURL + "/" + path

	resp, err := retryHTTPWithClient(ctx, client, url, retries)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d for %s", resp.StatusCode, path)
	}

	// Check context again before processing
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Decompress gzip
	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader for %s: %w", path, err)
	}
	defer gzReader.Close()

	// Extract URLs and send in batches to channel (with context check)
	if err := extractURLs(ctx, gzReader, urlChan); err != nil {
		return fmt.Errorf("failed to extract URLs from %s: %w", path, err)
	}

	return nil
}

func extractURLs(ctx context.Context, reader io.Reader, urlChan chan<- []byte) error {
	// Use large buffer for scanner (1MB) to reduce system calls
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 1024*1024) // 1MB initial capacity
	scanner.Buffer(buf, 10*1024*1024) // Allow up to 10MB lines

	// Batch URLs to reduce channel overhead
	var batch bytes.Buffer
	batch.Grow(100 * 1024) // Pre-allocate 100KB for batch

	// CommonCrawl index files are newline-delimited JSON (NDJSON)
	// Use regex extraction only (much faster than JSON parsing)
	for scanner.Scan() {
		// Check context periodically
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Fast regex extraction (skip JSON parsing entirely)
		url := extractURLWithRegexBytes(line)
		if len(url) > 0 {
			batch.Write(url)
			batch.WriteByte('\n')

			// Send batch when it reaches threshold
			if batch.Len() >= 64*1024 { // 64KB batches
				data := make([]byte, batch.Len())
				copy(data, batch.Bytes())
				select {
				case urlChan <- data:
				case <-ctx.Done():
					return ctx.Err()
				}
				batch.Reset()
				batch.Grow(100 * 1024)
			}
		}
	}

	// Send remaining URLs in batch
	if batch.Len() > 0 {
		data := make([]byte, batch.Len())
		copy(data, batch.Bytes())
		select {
		case urlChan <- data:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func extractURLWithRegexBytes(line []byte) []byte {
	// Fast regex extraction similar to grep -oP '(?<="url": ")[^"]+(?=")'
	// Using bytes for better performance (no string conversion)
	pattern := []byte(`"url": "`)
	startIdx := bytes.Index(line, pattern)
	if startIdx == -1 {
		return nil
	}
	startIdx += len(pattern)

	// Find the closing quote
	endIdx := bytes.IndexByte(line[startIdx:], '"')
	if endIdx == -1 {
		return nil
	}

	return line[startIdx : startIdx+endIdx]
}

// extractFilenameByExtension checks if URL path ends with any of the target extensions
// and returns the matched extension and filename. Returns empty string if no match.
func extractFilenameByExtension(urlBytes []byte) (string, []byte) {
	// Parse URL to check if path (not query) ends with any target extension
	urlStr := string(urlBytes)
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return "", nil
	}

	// Get the path part (without query parameters)
	path := parsedURL.Path
	if len(path) == 0 {
		return "", nil
	}

	// Convert to lowercase for case-insensitive matching
	pathLower := strings.ToLower(path)

	// Define all extensions to check (in one pass)
	extensions := []string{
		".php", ".aspx", ".asp", ".jsp", ".jspx", ".do", ".sql", ".log",
		".py", ".java", ".conf", ".cnf", ".ini", ".env", ".sh", ".bak",
		".backup", ".tar", ".yml", ".swp", ".old", ".svn", ".htpasswd",
		".htaccess", ".json", ".txt", ".pdf", ".xml", ".xls", ".xlsx",
		".ppt", ".pptx", ".doc", ".docx", ".csv", ".db", ".js", ".zip",
	}

	// Check all extensions in one pass (efficient - no multiple iterations)
	for _, ext := range extensions {
		if strings.HasSuffix(pathLower, ext) {
			// Extract just the filename from the path
			filename := filepath.Base(path)
			if len(filename) == 0 || filename == "." || filename == "/" {
				return "", nil
			}

			// Verify filename actually ends with the extension (case-insensitive)
			filenameLower := strings.ToLower(filename)
			if strings.HasSuffix(filenameLower, ext) {
				return ext, []byte(filename)
			}
		}
	}

	return "", nil
}

// extractHostnameFromURL parses a URL and extracts the hostname (domain or IP)
// It handles port numbers by removing them, and returns the hostname string
func extractHostnameFromURL(urlBytes []byte) string {
	urlStr := string(urlBytes)
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}

	host := parsedURL.Host
	if host == "" {
		return ""
	}

	// Remove port number if present (e.g., "example.com:8080" -> "example.com")
	// Split by colon and take the first part
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	return host
}

// isValidDomain validates if a hostname is a valid domain/subdomain
// It checks for proper domain structure, valid characters, and filters out invalid entries
func isValidDomain(hostname string) bool {
	if hostname == "" {
		return false
	}

	// Must contain at least one dot (for TLD)
	if !strings.Contains(hostname, ".") {
		return false
	}

	// Check total length (max 253 characters for FQDN)
	if len(hostname) > 253 {
		return false
	}

	// Must not start or end with dot or hyphen
	if strings.HasPrefix(hostname, ".") || strings.HasSuffix(hostname, ".") {
		return false
	}
	if strings.HasPrefix(hostname, "-") || strings.HasSuffix(hostname, "-") {
		return false
	}

	// Split by dots to check each label
	labels := strings.Split(hostname, ".")
	if len(labels) < 2 {
		return false
	}

	// TLD (last label) must be at least 2 characters
	tld := labels[len(labels)-1]
	if len(tld) < 2 {
		return false
	}

	// Check each label
	hasLetter := false
	for _, label := range labels {
		// Each label must be 1-63 characters
		if len(label) == 0 || len(label) > 63 {
			return false
		}

		// Check for valid characters in label (letters, numbers, hyphens)
		for _, char := range label {
			// Check if character is valid (letter, digit, or hyphen)
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-') {
				return false
			}
			// Track if we have at least one letter
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
				hasLetter = true
			}
		}

		// Label must not start or end with hyphen
		if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
	}

	// Must contain at least one letter (not just numbers)
	if !hasLetter {
		return false
	}

	return true
}

func retryHTTP(url string, retries int) (*http.Response, error) {
	// Use a client with timeout for small files like index paths
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
	// Use background context for index path fetching (before main context is created)
	return retryHTTPWithClient(context.Background(), client, url, retries)
}

func retryHTTPWithClient(ctx context.Context, client *http.Client, url string, retries int) (*http.Response, error) {
	var lastErr error
	backoff := time.Second

	for attempt := 0; attempt <= retries; attempt++ {
		// Check context before each attempt
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if attempt > 0 {
			log.Printf("Retrying %s (attempt %d/%d)...", url, attempt, retries)
			// Use context-aware sleep
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			backoff *= 2 // Exponential backoff
		}

		// Create request with context
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		resp.Body.Close()
		lastErr = fmt.Errorf("status code %d", resp.StatusCode)
	}

	return nil, fmt.Errorf("failed after %d retries: %w", retries, lastErr)
}
