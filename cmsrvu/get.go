package cmsrvu

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"regexp"
)

var DefaultBaseUrl = "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/"
var ExampleSourceFile = "rvu17b.zip"
var DefaultFilenameRegex = `(?i)^pprrvu.*\.csv$`

// "PPRRVU17_V0209.csv"

// GetRecords is a high level function to get records from a zip file,
// and fall back to importing directly from source url
// if no file is found or cacheFile is "".
func GetRecords(srcUrl, cacheFile, pattern string) ([][]string, error) {

	records, err := CSVRecordsFromZipFile(cacheFile, pattern)
	if records != nil && err == nil {
		return records, err
	}

	data, err := DownloadZip(srcUrl)
	if err != nil {
		return nil, err
	}
	return CSVFromZip(data, pattern)
}

// DownloadZip takes a source url and returns the binary zip file as a byte slice.
// It can be used to directly import from url
func DownloadZip(srcUrl string) ([]byte, error) {
	resp, err := http.Get(srcUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// CSVFromZip returns parsed csv records from from zip data. It extracts the first
// file in an archive that matches pattern (using standard regexp matching).
func CSVFromZip(data []byte, pattern string) ([][]string, error) {

	//
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
			break
		}
	}

	// parse csv records
	rc, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return csv.NewReader(rc).ReadAll()
}

func FromZip() {}

// CSVRecordsFromZipFile wraps CSVFrom Zip to load from a zip file (as opposed to in memory data)
func CSVRecordsFromZipFile(file, pattern string) ([][]string, error) {

	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return CSVFromZip(data, pattern)
}

// ToFile feels frivolous
func ToFile(data []byte, fileName string) error {
	return os.WriteFile(fileName, data, 0644)
}
