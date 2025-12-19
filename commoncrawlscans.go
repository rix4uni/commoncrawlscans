package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
)

const (
	configDirName = ".config/commoncrawlscans"
)

const (
	baseURL = "https://data.commoncrawl.org"
)

type config struct {
	files   int
	output  string
	retries int
	resume  bool
}

type urlResult struct {
	path string
	err  error
}

func main() {
	cfg := parseFlags()

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
		outputDir = "commoncrawlscans" // Default directory
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}
	outputPath := filepath.Join(outputDir, "commoncrawlscans.txt")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()
	log.Printf("Writing output to %s", outputPath)

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
	processFiles(client, paths, cfg.files, cfg.retries, outputFile, extensionWriters, resumeFile, failedFile)

	log.Println("Processing complete")
}

func parseFlags() *config {
	cfg := &config{}
	pflag.IntVar(&cfg.files, "files", 1, "Number of files to process concurrently")
	pflag.StringVar(&cfg.output, "output", "", "Directory name to save output file (default: commoncrawlscans)")
	pflag.IntVar(&cfg.retries, "retries", 3, "Number of retry attempts for failed requests")
	pflag.BoolVar(&cfg.resume, "resume", false, "Resume from previous run using resume file")
	pflag.Parse()
	return cfg
}

func readCrawlVersion() (string, error) {
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return "", fmt.Errorf("no input provided")
	}
	version := scanner.Text()
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading from stdin: %w", err)
	}
	return version, nil
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

func processFiles(client *http.Client, paths []string, numWorkers int, retries int, outputWriter io.Writer, extensionWriters map[string]io.Writer, resumeFile, failedFile string) {
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

	// Create shared writer for output file with large buffer
	writer := bufio.NewWriterSize(outputWriter, 1024*1024) // 1MB buffer
	defer writer.Flush()

	// Create buffered writers for each extension file
	extensionBuffers := make(map[string]*bufio.Writer)
	var extensionMutex sync.Mutex

	for ext, writer := range extensionWriters {
		extensionBuffers[ext] = bufio.NewWriterSize(writer, 64*1024) // 64KB buffer
	}

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
		}()

		batchCount := 0
		for urls := range urlChan {
			// Write all URLs to main output
			writer.Write(urls)

			// Filter and extract filenames for all extensions in one pass
			lines := bytes.Split(urls, []byte{'\n'})
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				// Check all extensions in one efficient pass
				if matchedExt, filename := extractFilenameByExtension(line); matchedExt != "" {
					extensionMutex.Lock()
					if bufWriter, ok := extensionBuffers[matchedExt]; ok {
						bufWriter.Write(filename)
						bufWriter.WriteByte('\n')
					}
					extensionMutex.Unlock()
				}
			}

			batchCount++
			// Flush every 100 batches to ensure data is written
			if batchCount%100 == 0 {
				writer.Flush()
				extensionMutex.Lock()
				for _, bufWriter := range extensionBuffers {
					bufWriter.Flush()
				}
				extensionMutex.Unlock()
			}
		}
		writer.Flush() // Final flush
		extensionMutex.Lock()
		for _, bufWriter := range extensionBuffers {
			bufWriter.Flush()
		}
		extensionMutex.Unlock()
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
