package main

import (
	"bufio"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"strconv"
	"sync"
	"time"

	// Load the drivers
	// MySQL
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type config struct {
	connString    string
	db            string
	pauseInterval time.Duration
	workers       int
	debugMode     bool
	loops         int
}

type result struct {
	start     time.Time
	end       time.Time
	dbTime    time.Duration
	workCount int
	errors    int
}

var format = "%10s  %20s  %20s  %10s %4s  %10s %4s   %16s\n"

func print_separator(c string) {
	fmt.Printf("%s\n", strings.Repeat(c, 120))
}

func print_header() {
	print_separator("=")
	fmt.Printf(format, "", "Wall time (s)", "DB time (s)", "Requests", "%%", "Errors", "%%", "Requests/sec")
	print_separator("-")
}

func print_row(title string, r result, total result) {
	wall_dur := r.end.Sub(r.start)
	fmt.Printf(format, title,
			strconv.FormatFloat(wall_dur.Seconds(), 'f', 3, 32),
			strconv.FormatFloat(r.dbTime.Seconds(), 'f', 3, 32),
			strconv.Itoa(r.workCount), strconv.Itoa(int(100 * float64(r.workCount)/float64(total.workCount))),
			strconv.Itoa(r.errors), strconv.Itoa(int(100 * float64(r.errors)/float64(total.workCount))),
			strconv.FormatFloat(float64(r.workCount)/float64(wall_dur.Seconds()), 'f', 2, 64))
}

func main() {
	cfg, err := getConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Open the connection to the DB - should each worker open it's own connection?
	// Food for thought
	db, err := sql.Open(cfg.db, cfg.connString)
	if err != nil {
		log.Fatal(err)
	}

	// Check the database connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Declare the channel we'll be using as a work queue
	inputChan := make(chan (string))
	// Declare the channel that will gather results
	outputChan := make(chan (result), cfg.workers)
	// Declare a waitgroup to help prevent log interleaving - I technically do not
	// need one, but without it, I find there are stray log messages creeping into
	// the final report. Setting sync() on STDOUT/ERR didn't seem to fix it.
	var wg sync.WaitGroup
	wg.Add(cfg.workers)

	var total result

	// Start the pool of workers up, reading from the channel
	total.start = time.Now()
	fmt.Printf("Current date: %s\n", total.start.Format("2006-01-02 15:04:05"))
	fmt.Printf("Running the slammer...\n\n")

	startWorkers(cfg.workers, inputChan, outputChan, db, &wg, cfg.pauseInterval, cfg.debugMode, cfg.loops)

	// Warm up error and line so I can use error in the for loop with running into
	// a shadowing issue
	err = nil
	line := ""
	// Read from STDIN in the main thread
	input := bufio.NewReader(os.Stdin)
	for err != io.EOF {
		line, err = input.ReadString('\n')
		if err == nil {
			// Get rid of any unwanted stuff
			line = strings.TrimRight(line, "\r\n")
			// Push that onto the work queue
			inputChan <- line
			total.workCount += cfg.loops
		} else if cfg.debugMode {
			log.Println(err)
		}
	}

	// Close the channel, since it's done receiving input
	close(inputChan)
	// As I mentioned above, because workers wont finish at the same time, I need
	// to use a waitgroup, otherwise the output below gets potentially mixed in with
	// debug or error messages from the workers. The waitgroup semaphore prevents this
	// even though it probably looks redundant
	wg.Wait()
	total.end = time.Now()
	// Collect all results, report them. This will block and wait until all results
	// are in
	print_header()
	for i := 0; i < cfg.workers; i++ {
		r := <-outputChan
		total.errors += r.errors
		total.dbTime += r.dbTime
		print_row("worker#" + strconv.Itoa(i), r, total)
	}
	print_separator("-")
	print_row("Overall", total, total)

	// Lets just be nice and tidy
	close(outputChan)
}

func startWorkers(count int, ic <-chan string, oc chan<- result, db *sql.DB, wg *sync.WaitGroup, pause time.Duration, debugMode bool, loops int) {
	// Start the pool of workers up, reading from the channel
	for i := 0; i < count; i++ {
		// register a signal chan for handling shutdown
		sc := make(chan os.Signal)
		signal.Notify(sc, os.Interrupt)
		// Pass in everything it needs
		go startWorker(i, ic, oc, sc, db, wg, pause, debugMode, loops)
	}
}

func startWorker(workerNum int, ic <-chan string, oc chan<- result, sc <-chan os.Signal, db *sql.DB, done *sync.WaitGroup, pause time.Duration, debugMode bool, loops int) {
	// Prep the result object
	r := result{start: time.Now()}
	for line := range ic {
		// First thing is first - do a non blocking read from the signal channel, and
		// handle it if something came through the pipe
		select {
		case _ = <-sc:
			// UGH I ACTUALLY ALMOST USED A GOTO HERE BUT I JUST CANT DO IT
			// NO NO NO NO NO NO I WONT YOU CANT MAKE ME NO
			// I could put it into an anonymous function defer, though...
			r.end = time.Now()
			oc <- r
			done.Done()
			return
		default:
			// NOOP
		}

		for i := 0; i < loops; i++ {
			t := time.Now()
			_, err := db.Exec(line)
			r.dbTime += time.Since(t)
			// TODO should this be after the err != nil? It counts towards work attempted
			// but not work completed.
			r.workCount++
			if err != nil {
				r.errors++
				if debugMode {
					log.Printf("Worker #%d: %s - %s", workerNum, line, err.Error())
				}
			} else {
				// Sleep for the configured amount of pause time between each call
				time.Sleep(pause)
			}
		}
	}

	// Let everyone know we're done, and bail out
	r.end = time.Now()
	oc <- r
	done.Done()
}

func getConfig() (*config, error) {
	p := flag.String("p", "1s", "The time to pause between each call to the database")
	c := flag.String("c", "", "The connection string to use when connecting to the database")
	db := flag.String("db", "mysql", "The database driver to load. Defaults to mysql")
	w := flag.Int("w", 1, "The number of workers to use. A number greater than 1 will enable statements to be issued concurrently")
	n := flag.Int("n", 1, "The number of loops to perform in each worker (default is 1)")
	d := flag.Bool("d", false, "Debug mode - turn this on to have errors printed to the terminal")
	// TODO support an "interactive" flag to drop you into a shell that outputs things like
	// sparklines of the current worker throughputs
	flag.Parse()

	if *c == "" {
		return nil, errors.New("You must provide a connection string using the -c option")
	}
	pi, err := time.ParseDuration(*p)
	if err != nil {
		return nil, errors.New("You must provide a proper duration value with -p")
	}

	if *w <= 0 {
		return nil, errors.New("You must provide a worker count > 0 with -w")
	}

	return &config{db: *db, connString: *c, pauseInterval: pi, workers: *w, debugMode: *d, loops: *n}, nil
}
