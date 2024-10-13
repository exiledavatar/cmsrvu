package cmsrvu

import "errors"

var DefaultRVUFileRegex = `(?i)^pprrvu.*\.csv$`

type RelativeValueUnits []RelativeValueUnit

func (r RelativeValueUnits) ToFile(file string) error {
	return errors.New("TODO")
}

func GetRVUs(srcUrl, cacheFile, pattern string) (RelativeValueUnits, error) {

	if pattern == "" {
		pattern = DefaultRVUFileRegex
	}

	records, err := GetRecords(srcUrl, cacheFile, pattern)
	if err != nil {
		return nil, err
	}

	rvus := []RelativeValueUnit{}
	for _, r := range records {
		rvu, err := RVUFromRecord(r)
		if err != nil {
			return nil, err
		}
		rvus = append(rvus, rvu)
	}

	return rvus, nil
}
