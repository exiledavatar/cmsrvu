package cmsrvu

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

var DefaultBaseUrl = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/"
var ExampleSourceFile = "rvu17b.zip"
var DefaultFilenameRegex = `(?i)^pprrvu.*\.csv$`

// "PPRRVU17_V0209.csv"

type Records struct {
	Data []any
	Meta map[string]any
}

// GetRecords is a high level function to get records from a zip file, and fall back
// to importing directly from source url if there is any error getting it from the file.
func GetRecords(srcUrl, cacheFile, pattern string) ([][]string, map[string]any, error) {

	zippedData, headers, err := Download(srcUrl)
	if err != nil {
		return nil, nil, err
	}
	// fmt.Println("Downloaded data: ", srcUrl, " - ", len(zippedData))
	meta := map[string]any{}
	lastModified, err := time.Parse(time.RFC1123, headers["Last-Modified"][0])
	if err != nil {
		return nil, nil, err
	}
	extractTime, err := time.Parse(time.RFC1123, headers["Date"][0])
	if err != nil {
		return nil, nil, err
	}
	meta["last-modified"] = lastModified.UTC()
	meta["extract-time"] = extractTime.UTC()
	meta["source"] = srcUrl

	records, err := CSVFromZip(zippedData, pattern)
	// fmt.Println("Got records from data: ", len(records))
	return records, meta, err
}

// Download is a simple wrapper that reads the response to a byte slice and returns it
// along with the response headers
func Download(srcUrl string) ([]byte, http.Header, error) {
	resp, err := http.Get(srcUrl)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	return data, resp.Header, err
}

// CSVFromZip returns parsed csv records from from zip data. It extracts the first
// file in an archive that matches pattern (using standard regexp matching).
func CSVFromZip(data []byte, pattern string) ([][]string, error) {

	// open a zip file handler
	zipReader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	// get first file in zip that matches pattern
	pat, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	var zipFile *zip.File
	for _, f := range zipReader.File {
		if pat.MatchString(f.Name) {
			zipFile = f
			fmt.Println(f.Name)
			break
		}
	}

	// parse csv records
	rc, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// read to a []byte
	bd, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	// scrub all the funky characters
	// cbd := cleanBytes(bd)
	// cbr := bytes.NewReader(cbd)

	// someone at CMS decided to change how they save their CSV's - hopefully this addresses the issue...
	// but consider moving to the txt files as they supposedly guarantee consistent formatting
	bd = bytes.ReplaceAll(bd, []byte("\r"), []byte("\n"))

	// fmt.Printf("%#v\n", string(bd[0:1000]))
	cbr := bytes.NewReader(bd)
	// convert []byte to an io.Reader
	// fmt.Println(zipFile.FileHeader)
	// fmt.Println(zipFile.FileInfo())
	csvReader := csv.NewReader(cbr)
	// csvReader.FieldsPerRecord = 31

	// this loop is to burn through the junk rows and the header
	for i := range 20 {
		// fmt.Println(i)
		// record, err := csvReader.Read()
		// if err != nil {
		// 	log.Println(err)
		// 	// return nil, err
		// }
		// if len(record) == 0 {
		// 	continue
		// }
		// if lastHeader, err := regexp.MatchString("(?i)hcpcs$", record[0]); lastHeader {
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	break
		// }

		switch record, _ := csvReader.Read(); {
		case len(record) == 0:
			// fmt.Println("this is zero length")
			//do nothing
		case record[0] == "HCPCS":
			// fmt.Println(i, " found the header row")
			break
		case i > 20:
			return nil, fmt.Errorf("cannot find header in first 20 rows, check file")
		default:
			// fmt.Println("me? ", i, "\t", record[0])
		}
	}

	records, err := csvReader.ReadAll()

	if err != nil {
		fmt.Println("Error reading records: ", err)

		return nil, err
	}
	// fmt.Println(len(records))
	// fmt.Printf("%#v+\n", records)
	// fmt.Println(len(records[0]))

	// fmt.Printf("%v\n", records[0][571169])

	// if len(records) == 1 && len(records[0]) > 1 {
	// 	// records = records[0]

	// }

	// for _, v := range records {
	// 	// fmt.Println(i, "\n")
	// 	for j, vj := range v {
	// 		fmt.Println("\t", j, "\t", vj, "wtf")
	// 		if j > 1000 {
	// 			break
	// 		}
	// 	}
	// }
	// fmt.Printf("%+v\n", records[1][0:5])

	return records, err
}

// func cleanBytes(data []byte) []byte {
// 	// let's be extra certain
// 	d := bytes.ToValidUTF8(data, []byte{})
// 	d = bytes.ReplaceAll(d, []byte{'\x00'}, []byte{})

// 	return bytes.Map(func(r rune) rune {
// 		switch {
// 		case !utf8.ValidRune(r):
// 			return -1
// 		case !unicode.IsPrint(r):
// 			return -1
// 		default:
// 			return r
// 		}
// 	},
// 		d)
// }
