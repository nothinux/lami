package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"os"
)

var (
	rules = map[string]string{
		"Time":                  `datetime`,
		"Schema":                `string`,
		"Query_time":            `time`,
		"Lock_time":             `time`,
		"Rows_sent":             `int`,
		"Rows_examined":         `int`,
		"Rows_affected":         `int`,
		"Rows_read":             `int`,
		"Bytes_sent":            `int`,
		"Tmp_tables":            `int`,
		"Tmp_disk_tables":       `int`,
		"Tmp_table_sizes":       `int`,
		"QC_Hit":                `bool`,
		"Full_scan":             `bool`,
		"Full_join":             `bool`,
		"Tmp_table":             `bool`,
		"Tmp_table_on_disk":     `bool`,
		"Filesort":              `bool`,
		"Filesort_on_disk":      `bool`,
		"Merge_passes":          `int`,
		"InnoDB_IO_r_ops":       `int`,
		"InnoDB_IO_r_bytes":     `int`,
		"InnoDB_IO_r_wait":      `time`,
		"InnoDB_rec_lock_wait":  `time`,
		"InnoDB_queue_wait":     `time`,
		"InnoDB_pages_distinct": `int`,
	}

	regexps = map[string]*regexp.Regexp{}
)

type Record map[string]interface{}

func compileRegexps() {
	for key, rule := range rules {
		var data string

		switch rule {
		case "datetime":
			data = `.*`
		case "string":
			data = `\w+`
		case "time":
			data = `[0-9\.]+`
		case "int":
			data = `\d+`
		case "bool":
			data = `\w+`
		default:
			panic("uknown rule: " + rule)
		}

		regexps[key] = regexp.MustCompile(
			`^# .*` + key + `: (` + data + `)`,
		)
	}
}

func Usage(showUsage bool) {
	if showUsage {
		flag.Usage()
	}
}

func main() {
	showHelp := flag.Bool("h", false, "show help")
	file := flag.String("f", "", "specify file` location to read")

	flag.Parse()

	if *showHelp {
		Usage(true)
	}

	if *file != "" {
		Run(*file)
	}

}

func Run(filepath string) {
	compileRegexps()

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var (
		reader  = bufio.NewReader(file)
		record  = Record{}
		records = []Record{}
	)

	var line string
	for {
		data, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatalln("can't read input data")
		}

		if isPrefix {
			line += string(data)

			continue
		}

		line = string(data)

		if strings.HasPrefix(line, "# Time: ") {
			if len(record) > 0 {
				if record, ok := process(record); ok {
					records = append(records, record)
				}
			}

			record = Record{}
		}

		err = unmarshal(line, record)
		if err != nil {
			log.Println(err)
		}
	}

	if record, ok := process(record); ok {
		records = append(records, record)
	}

	for index, record := range records {
		records[index] = prepare(record)
	}

	for i := 0; i < len(records); i++ {
		data, err := json.Marshal(records[i])
		if err != nil {
			log.Fatalln("unable to encode records to JSON")
		}

		if err := savetofile(filepath, string(data)); err != nil {
			log.Fatal(err)
		}
	}
}

func savetofile(output, data string) error {
	s := strings.Split(output, ".")
	filename := fmt.Sprintf("%s.json", s[0])
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	fmt.Fprintf(file, "%s\n", data)
	return nil
}

func process(record Record) (Record, bool) {
	if timeEnd, ok := record["time"].(time.Time); ok {
		if queryTime, ok := record["query_time"].(time.Duration); ok {
			record["time_start"] = timeEnd.Add(queryTime * -1)
		}
	} else {
		return record, false
	}

	if query, ok := record["query"].(string); ok {
		record["query_length"] = len(query)

		record["query_type"] = getQueryType(query)
	}

	return record, true
}

func prepare(record Record) Record {
	for key, value := range record {
		switch value := value.(type) {
		case time.Time:
			record[key] = value.Format("2006-01-02 15:04:05.00000000")

		case time.Duration:
			record[key] = value.Seconds()
		}
	}

	return record
}

func unmarshal(line string, record Record) error {
	if !strings.HasPrefix(line, "# ") {
		_, ok := record["query"]
		if ok {
			record["query"] = record["query"].(string) + line
			return nil
		}

		record["query"] = line
	}

	for key, rule := range rules {
		raw, ok := match(line, key)
		if !ok {
			continue
		}

		value, err := parse(raw, key, rule)
		if err != nil {
			log.Fatalf("unable to parse %s: %s", key, raw)
		}

		record[strings.ToLower(key)] = value
	}

	return nil
}

func match(data, key string) (string, bool) {
	matches := regexps[key].FindStringSubmatch(data)
	if len(matches) > 0 {
		return matches[1], true
	}

	return "", false
}

func parse(raw, key, rule string) (interface{}, error) {
	switch rule {
	case "datetime":
		return time.Parse("060102 15:04:05", raw)
	case "time":
		return time.ParseDuration(raw + "s")
	case "string":
		return raw, nil
	case "int":
		return strconv.ParseInt(raw, 10, 64)
	case "bool":
		switch raw {
		case "Yes":
			return true, nil
		case "No":
			return false, nil
		default:
			return false, errors.New("invalid syntax: expected Yes or No")
		}
	}

	return nil, nil
}

func getQueryType(query string) string {
	operations := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"DROP",
	}

	min := -1
	queryType := ""
	for _, operation := range operations {
		index := strings.Index(query, operation)
		if index >= 0 && (index <= min || min == -1) {
			queryType = operation
			min = index
		}
	}

	if min >= 0 {
		return queryType
	}

	return ""
}
