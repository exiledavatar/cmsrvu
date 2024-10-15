package cmsrvu

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type Config struct {
	BaseURL      string
	RVUFileRegex string
	Data         []DataConfig
	DB           DBConfig
}

type DataConfig struct {
	EffectiveDate pgtype.Date
	URL           string
	FileRegex     string // filenames in the zip archives aren't always the most consistent, this will override the default Config.RVUFileRegex
}

type DBConfig struct {
	ConnectionString string
	User             string
	Password         string `yaml:"-"`
}

var DefaultConfig = Config{
	BaseURL:      "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/",
	RVUFileRegex: `(?i)^pprrvu.*\.csv$`,
	DB: DBConfig{
		ConnectionString: "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable",
		User:             "postgres",
		Password:         "password",
	},
	Data: []DataConfig{
		{EffectiveDate: parseDate("2015-01-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu15a.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2015-04-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu15b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2015-07-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu15c.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2015-10-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu15d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2016-01-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu16a.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2016-04-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu16b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2016-07-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu16c.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2016-10-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu16d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2017-01-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu17a.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2017-04-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu17b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2017-07-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu17c.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2017-10-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu17d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2018-01-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu18ar1.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2018-04-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu18b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2018-07-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu18c1.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2018-10-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu18d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2019-01-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu19a.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2019-04-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu19b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2019-07-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu19c.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2019-10-01"), URL: "https://www.cms.gov/medicare/medicare-fee-for-service-payment/physicianfeesched/downloads/rvu19d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2020-01-01"), URL: "https://www.cms.gov/files/zip/rvu20a-updated-01312020.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2020-04-01"), URL: "https://www.cms.gov/files/zip/rvu20b-updated-05012020.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2020-07-01"), URL: "https://www.cms.gov/files/zip/rvu20c-updated-06192020.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2020-10-01"), URL: "https://www.cms.gov/files/zip/rvu20d-updated-11232020.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2021-01-01"), URL: "https://www.cms.gov/files/zip/rvu21a-updated-01052021.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2021-04-01"), URL: "https://www.cms.gov/files/zip/rvu21b-updated-03022021.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2021-07-01"), URL: "https://www.cms.gov/files/zip/rvu21c-updated-6302021.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2021-10-01"), URL: "https://www.cms.gov/files/zip/rvu21d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2022-01-01"), URL: "https://www.cms.gov/files/zip/rvu22a.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2022-04-01"), URL: "https://www.cms.gov/files/zip/rvu22b.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2022-07-01"), URL: "https://www.cms.gov/files/zip/rvu22c-updated-06172022.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2022-10-01"), URL: "https://www.cms.gov/files/zip/rvu22d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2023-01-01"), URL: "https://www.cms.gov/files/zip/rvu23a-updated-01/31/2023.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2023-04-01"), URL: "https://www.cms.gov/files/zip/rvu23b-updated-02/27/2023.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2023-07-01"), URL: "https://www.cms.gov/files/zip/rvu23c.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2023-10-01"), URL: "https://www.cms.gov/files/zip/rvu23d.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2024-01-01"), URL: "https://www.cms.gov/files/zip/rvu24ar-posted-04/01/2024.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2024-04-01"), URL: "https://www.cms.gov/files/zip/rvu24b-updated-03/18/2024.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2024-07-01"), URL: "https://www.cms.gov/files/zip/rvu24c-updated-09/09/2024.zip", FileRegex: ""},
		{EffectiveDate: parseDate("2024-10-01"), URL: "https://www.cms.gov/files/zip/rvu24d.zip", FileRegex: ""},
	},
}

func parseDate(s string) pgtype.Date {
	date, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return pgtype.Date{
		Time:             date,
		InfinityModifier: pgtype.Finite,
		Valid:            true,
	}
}
