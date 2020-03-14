package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

const(
	DB_HOST string = "localhost:3306"
	DB_USER string = "root"
	DB_PWD  string = "1234qwer"
	DB_NAME string = "cs223_proj1"

	SQL_ROOT_PATH string = "/Users/xiaoru_zhu/Desktop/University_Courses/Tmp_CS223_proj/project1/"
	SQL_FLUSH_DB_PATH string = "schema/refresh_db_mysql.sql"

	SQL_INSERTION_LOW_META_PATH string = "data/low_concurrency/metadata.sql"
	SQL_INSERTION_LOW_OBSV_PATH string = "data/low_concurrency/observation_low_concurrency.sql"
	SQL_INSERTION_LOW_SMTC_PATH string = "data/low_concurrency/semantic_observation_low_concurrency.sql"

	SQL_INSERTION_HIGH_META_PATH string = "data/high_concurrency/metadata.sql"
	SQL_INSERTION_HIGH_OBSV_PATH string = "data/high_concurrency/observation_high_concurrency.sql"
	SQL_INSERTION_HIGH_SMTC_PATH string = "data/high_concurrency/semantic_observation_high_concurrency.sql"

	SQL_QUERY_LOW_PATH string = "queries/low_concurrency/queries.txt"
	SQL_QUERY_HIGH_PATH string = "queries/high_concurrency/queries.txt"
)


var ISOLEVEL = [4]sql.IsolationLevel {sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable}


// Flush database before insertion
func FlushDB(db *sql.DB, rootPath string){

	fileObj, err := os.Open(rootPath + SQL_FLUSH_DB_PATH)
	if err != nil { log.Fatal(err) }
	rawContent, err := ioutil.ReadAll(fileObj)
	if err != nil { log.Fatal(err) }

	fileObj.Close()

	if err := db.Ping(); err != nil { log.Fatal(err) }

	if _, err = db.Exec(string(rawContent)); err != nil { log.Fatal(err) }

}

// Metadata Insertion
func InsertMetadata(db *sql.DB, filePath string) {

	fileObj, err := os.Open(filePath)
	if err != nil { log.Fatal(err) }
	rawContent, err := ioutil.ReadAll(fileObj)
	if err != nil { log.Fatal(err) }

	fileObj.Close()

	reg := regexp.MustCompile(`--.*?\n|SET.*?\n`)
	content := reg.ReplaceAllString(string(rawContent), "")

	if err := db.Ping(); err != nil { log.Fatal(err) }

	if _, err = db.Exec(content); err != nil { log.Fatal(err) }

}

// Read Data from .sql file into memory, and preprocess the rows and split
func ReadCreateDropTableCommands(filePath string) []string {
	fileObj, err := os.Open(filePath)
	if err != nil { log.Fatal(err) }
	defer fileObj.Close()

	rawContent, err := ioutil.ReadAll(fileObj)
	if err != nil { log.Fatal(err) }
	reg:= regexp.MustCompile(`/\*.*?\*/`)
	content := reg.ReplaceAllString(string(rawContent), "")
	requests := strings.Split(content, "\n")
	return requests
}

func ReadInsertionCommands(filePath string) ([]string, []string) {
	fileObj, err := os.Open(filePath)
	if err != nil { log.Fatal(err) }
	defer fileObj.Close()

	rawContent, err := ioutil.ReadAll(fileObj)
	if err != nil { log.Fatal(err) }

	reg := regexp.MustCompile(`--.*?\n`)
	content := reg.ReplaceAllString(string(rawContent), "")
	requests := strings.Split(content, "\n")

	var setReqs, insertReqs []string
	for _, request := range requests{
		// Match set commands
		if regexp.MustCompile(`^SET.*;$`).MatchString(request) { setReqs = append(setReqs, request) }
		// Match insertion commands
		if regexp.MustCompile(`^INSERT[\s\S]*;$`).MatchString(request) { insertReqs = append(insertReqs, request) }
	}

	return setReqs, insertReqs
}


func ReadQueryCommands(filePath string) ([]time.Time, []string) {
	fileObj, err := os.Open(filePath)
	if err != nil { log.Fatal(err) }
	defer fileObj.Close()

	// Read from file to a string buffer
	rawContent, err := ioutil.ReadAll(fileObj)
	if err != nil { log.Fatal(err) }

	regComment := regexp.MustCompile(`--.*?\n`) // return type: *regexp.Regexp
	content := regComment.ReplaceAllString(string(rawContent), "")

	// Parse time stamp info
	regTimeStmp := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`) // return type: *regexp.Regexp
	strTimeStmps := regTimeStmp.FindAllString(content, -1)
	var stmps []time.Time
	// Change the string type to time for every element in strTimeStmps
	for _, strStmp := range strTimeStmps {
		uTime, _ := time.Parse(time.RFC3339, strStmp) // RFC3339: const "2006-01-02T15:04:05Z07:00"
		stmps = append(stmps, uTime)
	}

	// Parse querying commands
	regQuery := regexp.MustCompile(`"(\nSELECT[\s\S]+?)"`)
	rawQueries := regQuery.FindAllStringSubmatch(content, -1) //?
	var queries []string
	for _, tmp_list := range rawQueries{
		queries = append(queries, tmp_list[1])
	}

	return stmps, queries
}


// Record the responding time for every insertion and query
func timeElapse() func(tc *time.Duration) {
	start := time.Now() // closure
	return func(tc *time.Duration) {
		*tc = time.Since(start)
	}
}

// Create transaction and execute SQL operations for insertion and query
// Each sql operation is a transaction
func ExecuteTrx(db *sql.DB, isoLevel sql.IsolationLevel, sqlCommand string, tc *time.Duration) {
	defer timeElapse()(tc)

	if err := db.Ping(); err != nil{ log.Fatal(err) }
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: isoLevel,
		ReadOnly: false,
	})
	if err != nil { log.Fatal(err) }

	rst, err := tx.Exec(sqlCommand)
	if err != nil { log.Fatal(err) }

	if _, err = rst.RowsAffected(); err != nil { log.Fatal(err) }

	if err = tx.Commit(); err != nil{ log.Fatal(err) }
}

// Set multithread pool for the concurrency of the SQL operations
func ParallelExecuteInsertion(db *sql.DB, threadNum int, level sql.IsolationLevel, relativeFilePath string)  {
	if err := db.Ping(); err != nil{
		fmt.Println("Error: Failed to connect the Mysql database.")
		log.Fatal(err)
	}

	var barrier sync.WaitGroup
	// var mutex sync.Mutex

	var setReqs, insertReqs[]string
	setReqs, insertReqs = ReadInsertionCommands(SQL_ROOT_PATH + relativeFilePath)

	// "Set" command (does not work in mysql)
	// mutex.Lock()
	for _, request := range setReqs {
		// var tmp_dur time.Duration
		// ExecuteTrx(db, ISOLEVEL[0], request, &tmp_dur)
		fmt.Println(request)
	}
	// mutex.Unlock()

	var toatlTime time.Duration
	t0 := time.Now()
	fmt.Println("Timer start...")

	// Insertion command
	ch := make(chan time.Duration)
	var execTimeSet []time.Duration
	barrier.Add(1)
	go func() {
		// mutex.Lock()
		// defer mutex.Unlock()
		defer barrier.Done()
		fmt.Println("Parallel insertion start...")
		var groupSize = len(insertReqs) / threadNum

		for i := 0; i <= threadNum; i++ {
			offset := i
			barrier.Add(1)
			// fmt.Println(i)
			go func(offs int) {
				for j := offs * groupSize; j < (offs + 1) * groupSize; j++ {
					if j == len(insertReqs) { break }
					var tmpDuration time.Duration
					ExecuteTrx(db, level, insertReqs[j], &tmpDuration)

					// Passing the address by channel, but golang compiler will automatically
					//  decide to assign the local variable on heap or stack area
					//  which is diff from C/C++
					// Sending time data
					ch <- tmpDuration
				}
				barrier.Done()
			}(offset)
		}

	}()

	// Synchronization block
	go func(){
		// fmt.Println("1......debug")
		barrier.Wait()
		// fmt.Println("2......debug")
		close(ch)
		// fmt.Println("2......debug")
		toatlTime = time.Since(t0)
		fmt.Println("Parallel insertion ends.")
	}()

	// Receive the time data for every trx
	for duration := range ch{
		execTimeSet = append(execTimeSet, duration)
	}

	// fmt.Println("1200: ", execTimeSet[1200])
	// TODO: compute the average exec time
	// total time exec
	fmt.Println("total time: ", toatlTime)

}

func ParallelExecuteQuery(db *sql.DB, threadNum int, level sql.IsolationLevel, relativeFilePath string) {
	if err := db.Ping(); err != nil{
		fmt.Println("Error: Failed to connect the Mysql database.")
		log.Fatal(err)
	}

	fmt.Println("thread number: %d does not work", threadNum)

	// Read querying data
	timeStmps, queries := ReadQueryCommands(SQL_ROOT_PATH + relativeFilePath)

	// Group division
	var groupIdxs = make([]int, 1) // init: [0]
	var tmpTime time.Time = timeStmps[0]
	for idx, stmp := range timeStmps{
		if tmpTime.Date() == stmp.Date(){
			continue
		}else{
			tmpTime = stmp
			groupIdxs = append(groupIdxs, idx)
		}
	}
	groupIdxs = append(groupIdxs, len(timeStmps)) // last insertion: [0, ... , len(list)]

	var barrier sync.WaitGroup
	var ch = make(chan time.Duration)
	// var mutex sync.Mutex
	//
	for i := 0; i < len(groupIdxs) - 1; i++{
		barrier.Add(1)
		idx := i
		go func() {
			tNow := time.Now()
			defer barrier.Done()
			for j := groupIdxs[idx]; j < groupIdxs[idx + 1]; j++{
				var tmp_dur time.Duration
				ExecuteTrx(db, level, queries[j], &tmp_dur)
				ch <- tmp_dur
			}

			// Scale 1 day to 1 minute
			if dur := time.Since(tNow); dur < time.Minute{
				time.Sleep(time.Minute - dur)
			}

		}()
	}

	go func() {
		barrier.Wait()
		close(ch)
	}()

	var timeDurs []time.Duration
	// var t time.Duration
	for timeDur := range ch{
		timeDurs = append(timeDurs, timeDur)
	}
	fmt.Println(timeDurs)

}

func main()  {

	// Set number of parallel computing cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Command argument setting
	threadNum := flag.Int("threadNum", 5, "Set number of concurrency cores")
	isoLevel := flag.Int("isoLevel", 0, "Isolation level") //range: 0 ~ 3
	// var name string
	// flag.StringVar(&name, "name", "123", "name")
	flag.Parse()

	// Set connection to database
	db, err := sql.Open("mysql", DB_USER+":"+DB_PWD+"@tcp("+DB_HOST+")/"+DB_NAME)
	if err != nil{ log.Fatal(err) }
	if err := db.Ping(); err != nil{
		fmt.Println("Error: Failed to connect the Mysql database.")
		log.Fatal(err)
	}
	defer db.Close()

	// Flush DB first
	FlushDB(db, SQL_ROOT_PATH)

	// Metadata insertion
	InsertMetadata(db, SQL_ROOT_PATH + SQL_INSERTION_LOW_META_PATH)

	// Parallel computing
	ParallelExecuteInsertion(db, *threadNum, ISOLEVEL[*isoLevel], SQL_INSERTION_LOW_OBSV_PATH)
	ParallelExecuteInsertion(db, *threadNum, ISOLEVEL[*isoLevel], SQL_INSERTION_LOW_SMTC_PATH)
	ParallelExecuteQuery(db, *threadNum, ISOLEVEL[*isoLevel], SQL_QUERY_LOW_PATH)
}
