/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/exiledavatar/cmsrvu/cmsrvu"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// "github.com/gocarina/gocsv"

func main() {

	// fileName := "PPRRVU24_JUL.csv"
	// file, err := os.Open(fileName)
	// if err != nil {
	// 	panic(err)
	// }
	// defer file.Close()

	// r := csv.NewReader(file)
	// records, err := r.ReadAll()
	// if err != nil {
	// 	panic(err)
	// }
	// records = records[10:]
	// fmt.Println(records[0])
	// records = records[:10]
	// rvus := cmsrvu.RelativeValueUnits{}
	// for _, record := range records {
	// 	rvu, err := cmsrvu.RVUFromRecord(record)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	rvus = append(rvus, rvu)
	// 	fmt.Printf("%#v\n\n", rvu)
	// }
	// fmt.Println(len(records), len(records[0]))

	// resp, err := http.Get("https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu17b.zip")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer resp.Body.Close()
	// data, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// zipReader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// pat, err := regexp.Compile(`(?i)^pprrvu.*\.csv$`)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// var zipFile *zip.File
	// for _, f := range zipReader.File {
	// 	if pat.MatchString(f.Name) {
	// 		zipFile = f
	// 	}
	// 	fmt.Println(f.Name, pat.MatchString(f.Name))
	// }
	// rc, err := zipFile.Open()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rc.Close()
	// csvb := csv.NewReader(rc)

	// rows, err := csvb.ReadAll()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// rows = rows[10:]
	// rows = rows[:10]

	// xrvus := cmsrvu.RelativeValueUnits{}
	// for _, row := range rows {
	// 	rvu, err := cmsrvu.RVUFromRecord(row)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	xrvus = append(xrvus, rvu)
	// 	fmt.Printf("%#v\n\n", rvu)
	// }

	// fmt.Println(len(xrvus))

	// cmd.Execute()

	ctx := context.Background()
	db, err := sqlx.Connect("postgres", "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable")
	if err != nil {
		log.Println(err)
	}
	res, err := cmsrvu.RelativeValueUnits{}.CreatePostgresTable(ctx, db, "cmsrvu", "rvu")
	if err != nil {
		log.Println(err)
	}
	fmt.Println(res)

	cfg := cmsrvu.DefaultConfig
	for _, cfgd := range cfg.Data {
		rvus, err := cmsrvu.GetRVUs(
			cfgd.URL,
			"",
			cmsrvu.DefaultRVUFileRegex,
			cfgd.EffectiveDate,
		)
		if err != nil {
			log.Println(err)
		}

		chunkSize := 1000
		for i := 0; i < len(rvus); i += chunkSize {
			j := i + chunkSize
			if j > len(rvus) {
				j = len(rvus)
			}

			values := rvus[i:j]
			fmt.Println(values.PutPostgres(db, "cmsrvu", "rvu"))
		}
	}
	// fmt.Println(len(rvus))
	// fmt.Println(res)
	// }

	// fmt.Println(rvus.PutPostgres(db, "cmsrvu", "rvu"))
}
