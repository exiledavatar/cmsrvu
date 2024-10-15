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
	// fmt.Println(zipFile.FileHeader)
	// fmt.Println(zipFile.FileInfo())
	records, err := csv.NewReader(rc).ReadAll()
	if err != nil {
		return nil, err
	}
	return records, err
}
