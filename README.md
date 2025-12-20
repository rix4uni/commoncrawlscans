## CommonCrawl Scanner

A high-performance Go tool for scanning CommonCrawl index files, extracting URLs, subdomains, and IP addresses, and filtering them by file extensions. This tool processes CommonCrawl data concurrently and efficiently extracts URLs matching specific file types, with automatic deduplication and domain validation.

## Features

- **High Performance**: Processes multiple files concurrently with optimized streaming
- **Memory Efficient**: Streams data directly to disk, avoiding memory accumulation
- **Resume Support**: Can resume interrupted scans using resume files
- **File Extension Filtering**: Automatically filters and saves URLs by 37+ file extensions
- **Auto-Fetch Latest Crawl**: Automatically fetches the latest crawl version when no input is provided
- **Subdomain & IP Extraction**: Extracts unique subdomains and IP addresses from URLs
- **Domain Validation**: Filters out invalid subdomains using strict validation rules
- **Deduplication**: Ensures all output files contain only unique entries (no duplicates)
- **Graceful Shutdown**: Responds to CTRL+C quickly with proper cleanup
- **Progress Tracking**: Logs which files are being processed in real-time
- **Error Handling**: Retries failed requests with exponential backoff
- **Flexible Exclusion**: Exclude specific file types or outputs using `--exclude` flag

## Installation

### Prerequisites

- Go 1.21 or later
- Internet connection for downloading CommonCrawl data

### Build

```
go mod tidy
go build -o commoncrawlscans commoncrawlscans
```

Or run directly:

```
commoncrawlscans
```

## Usage

### Basic Usage

The tool can read the crawl version from stdin, or automatically fetch the latest version if no input is provided:

```yaml
# With explicit crawl version
echo "CC-MAIN-2025-47" | commoncrawlscans

# Auto-fetch latest crawl version (no input needed)
commoncrawlscans
```

### With Options

```yaml
echo "CC-MAIN-2025-47" | commoncrawlscans --files 10 --output results --retries 5
```

### Resume Interrupted Scan

```yaml
echo "CC-MAIN-2025-47" | commoncrawlscans --resume
```

### Exclude File Types

Exclude specific file types from being created and processed:

```yaml
# Exclude subdomains, PHP files, and ZIP files
commoncrawlscans --exclude "subdomains,php,zip"
```

### Silent Mode

Run without displaying the banner:

```yaml
commoncrawlscans --silent
```

### Check Version

Print version information and exit:

```yaml
commoncrawlscans --version
```

## Command-Line Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--files` | int | 1 | Number of files to process concurrently |
| `--output` | string | `commoncrawlscans` | Directory name to save output files |
| `--retries` | int | 3 | Number of retry attempts for failed HTTP requests |
| `--resume` | bool | false | Resume from previous run using resume file |
| `--exclude` | string | `` | Comma-separated list of file types to exclude (e.g., "subdomains,php,zip") |
| `--silent` | bool | false | Silent mode - suppress banner display |
| `--version` | bool | false | Print version information and exit |

## Output Files

### Main Output

- **`commoncrawlscans.txt`**: Contains all extracted URLs (one per line)

### Extension-Specific Files

The tool automatically creates separate files for each matched file extension:

- `php.txt` - PHP files
- `aspx.txt` - ASP.NET files
- `asp.txt` - Classic ASP files
- `jsp.txt` - Java Server Pages
- `jspx.txt` - JSPX files
- `do.txt` - Java servlet files
- `sql.txt` - SQL files
- `log.txt` - Log files
- `py.txt` - Python files
- `java.txt` - Java source files
- `conf.txt` - Configuration files
- `cnf.txt` - Configuration files
- `ini.txt` - INI configuration files
- `env.txt` - Environment files
- `sh.txt` - Shell scripts
- `bak.txt` - Backup files
- `backup.txt` - Backup files
- `tar.txt` - TAR archives
- `yml.txt` - YAML files
- `swp.txt` - Vim swap files
- `old.txt` - Old files
- `svn.txt` - SVN files
- `htpasswd.txt` - Apache password files
- `htaccess.txt` - Apache configuration files
- `json.txt` - JSON files
- `txt.txt` - Text files
- `pdf.txt` - PDF documents
- `xml.txt` - XML files
- `xls.txt` - Excel files (old format)
- `xlsx.txt` - Excel files
- `ppt.txt` - PowerPoint files (old format)
- `pptx.txt` - PowerPoint files
- `doc.txt` - Word documents (old format)
- `docx.txt` - Word documents
- `csv.txt` - CSV files
- `db.txt` - Database files
- `zip.txt` - ZIP archives

Each extension file contains only the **filename** (not the full URL) of matching files. All files are deduplicated, ensuring only unique entries are saved.

**Example:**
- URL: `http://example.com/path/to/file.zip`
- Saved to `zip.txt` as: `file.zip`

### Subdomain and IP Files

The tool also extracts and saves subdomains and IP addresses:

- **`subdomains.txt`**: Contains unique subdomains extracted from URLs (e.g., `subdomain.example.com`)
- **`ips.txt`**: Contains unique IP addresses extracted from URLs (e.g., `192.168.1.1`)

**Notes:**
- Subdomains are validated to ensure they are valid domain names (invalid entries are filtered out)
- IP addresses are detected using IPv4 parsing
- Both files are deduplicated (unique entries only)
- These files can be excluded using the `--exclude` flag (e.g., `--exclude "subdomains,ips"`)

### Resume and Failed Files

The tool creates tracking files in `~/.config/commoncrawlscans/`:

- **`{CRAWL_VERSION}.resume`**: List of successfully processed files (one path per line)
- **`{CRAWL_VERSION}.failed`**: List of files that failed to process (for debugging)

## How It Works

1. **Get Crawl Version**: Reads crawl version from stdin, or automatically fetches the latest version from Common Crawl website if no input is provided
2. **Fetch Index Paths**: Downloads the list of index files for the specified crawl version
3. **Filter for Resume**: If `--resume` is used, skips already processed files
4. **Concurrent Processing**: Downloads and processes multiple files in parallel
5. **Streaming Extraction**: Extracts URLs line-by-line and writes immediately to disk
6. **Extension Filtering**: Checks each URL's path (ignoring query parameters) against 37+ extensions
7. **Domain & IP Extraction**: Extracts hostnames from URLs, validates domains, and separates IPs from subdomains
8. **Deduplication**: Ensures all output files contain only unique entries using in-memory tracking
9. **Save Results**: Writes URLs to main file, filenames to extension-specific files, and subdomains/IPs to their respective files

## File Extension Matching Rules

The tool matches URLs based on the **path** part of the URL, not query parameters:

✅ **Correct matches:**
- `http://example.com/file.zip` → matches `.zip`
- `https://site.com/path/document.pdf` → matches `.pdf`
- `http://server.com/data.php?param=value` → matches `.php` (query ignored)

❌ **Incorrect matches (rejected):**
- `http://example.com/index.php?file=test.zip` → rejected (path is `index.php`, not `.zip`)

## Resume Functionality

The resume feature allows you to continue a scan that was interrupted:

1. **Automatic Tracking**: The tool automatically saves processed files to the resume file
2. **Resume on Restart**: Use `--resume` flag to skip already processed files
3. **Location**: Resume files are stored in `~/.config/commoncrawlscans/`

**Example:**
```yaml
# First run (processes 100 files, then interrupted)
echo "CC-MAIN-2025-47" | commoncrawlscans --files 5

# Resume run (skips the 100 processed files, continues with remaining)
echo "CC-MAIN-2025-47" | commoncrawlscans --files 5 --resume
```

## Performance Optimizations

- **Concurrent Processing**: Process multiple files simultaneously (controlled by `--files`)
- **Streaming**: URLs are written directly to disk, not accumulated in memory
- **Batched Writes**: URLs are batched (64KB) before writing to reduce I/O overhead
- **Large Buffers**: Uses 1MB buffers for output files, 64KB for extension files
- **Connection Pooling**: Reuses HTTP connections for better performance
- **Regex Extraction**: Fast byte-based URL extraction (no JSON parsing overhead)

## Examples

### Process with 10 concurrent workers

```yaml
echo "CC-MAIN-2025-47" | commoncrawlscans --files 10
```

### Save to custom directory

```yaml
echo "CC-MAIN-2025-47" | commoncrawlscans --output myresults
```

### Resume with custom settings

```yaml
echo "CC-MAIN-2025-47" | commoncrawlscans --resume --files 20 --retries 5
```

### Auto-fetch latest crawl version

```yaml
# Automatically fetches and uses the latest crawl version
commoncrawlscans
```

### Exclude specific file types

```yaml
# Exclude subdomains, PHP files, and ZIP files
commoncrawlscans --exclude "subdomains,php,zip"

# Exclude only IP addresses
commoncrawlscans --exclude "ips"
```

### Run in silent mode

```yaml
# Suppress banner display
commoncrawlscans --silent
```

### Check version

```yaml
# Print version information and exit
commoncrawlscans --version
```

### Check progress

The tool logs progress in real-time:
```
2025/12/19 20:10:08 Using crawl version: CC-MAIN-2025-47
2025/12/19 20:10:08 Fetching index paths...
2025/12/19 20:10:09 Found 300 .gz files to process
2025/12/19 20:10:09 Writing output to commoncrawlscans/commoncrawlscans.txt
2025/12/19 20:10:10 Processing file: cc-index/collections/CC-MAIN-2025-47/indexes/cdx-00000.gz
2025/12/19 20:10:15 Completed file: cc-index/collections/CC-MAIN-2025-47/indexes/cdx-00000.gz
2025/12/19 20:10:20 Progress: 10/300 files processed
```

## Graceful Shutdown

The tool handles CTRL+C gracefully:

1. **Immediate Response**: Stops assigning new work immediately
2. **Current Files**: Waits for current files to complete (up to 5 seconds)
3. **Save State**: Flushes resume file before exiting
4. **Force Exit**: If shutdown takes longer than 5 seconds, exits forcefully

Press CTRL+C once - the tool will attempt graceful shutdown. If it doesn't respond within 5 seconds, it will force exit.

## Troubleshooting

### High Memory Usage

- The tool streams data to disk, so memory usage should be low (~100MB)
- If you see high memory usage, reduce `--files` to process fewer files concurrently

### Slow Processing

- Increase `--files` to process more files concurrently
- Check your network connection speed
- CommonCrawl files can be large (hundreds of MBs), so downloads take time

### Failed Downloads

- The tool automatically retries failed requests (default: 3 retries)
- Increase `--retries` if you see many failures
- Check `{CRAWL_VERSION}.failed` file in `~/.config/commoncrawlscans/` for failed files

### Resume Not Working

- Ensure the resume file exists in `~/.config/commoncrawlscans/`
- Check that you're using the same crawl version
- The resume file format is one path per line

## File Structure

```yaml
.
├── commoncrawlscans.go    # Main source file
├── go.mod                 # Go module file
├── README.md             # This file
└── commoncrawlscans/      # Output directory (default)
    ├── commoncrawlscans.txt
    ├── subdomains.txt
    ├── ips.txt
    ├── php.txt
    ├── zip.txt
    ├── pdf.txt
    └── ... (other extension files)

~/.config/commoncrawlscans/  # Config directory
    ├── CC-MAIN-2025-47.resume
    └── CC-MAIN-2025-47.failed
```

## Technical Details

- **Language**: Go 1.21+
- **Dependencies**: `github.com/spf13/pflag` for CLI flags
- **HTTP Client**: Custom client with 2-hour timeout for large file downloads
- **Concurrency**: Worker pool pattern with buffered channels
- **I/O**: Buffered writers with periodic flushing

## License

This tool is provided as-is for educational and research purposes.

## Contributing

Contributions are welcome! Please ensure your code follows Go best practices and includes appropriate error handling.

