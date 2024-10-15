package cmsrvu

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jmoiron/sqlx"
)

var DefaultRVUFileRegex = `(?i)^pprrvu.*\.csv$`

type RelativeValueUnits []RelativeValueUnit

func GetRVUs(srcUrl, cacheFile, pattern string, effectiveDate pgtype.Date) (RelativeValueUnits, error) {
	if !effectiveDate.Valid {
		return nil, errors.New("valid effectiveDate required")
	}

	if pattern == "" {
		pattern = DefaultRVUFileRegex
	}

	records, md, err := GetRecords(srcUrl, cacheFile, pattern)
	if err != nil {
		return nil, err
	}

	source := (md["source"]).(string)
	lastModified := (md["last-modified"]).(time.Time)
	extractTime := (md["extract-time"]).(time.Time)
	rvus := []RelativeValueUnit{}

	records = records[10:]
	for _, r := range records {
		rvu, err := RVUFromRecord(r)
		if !rvu.StatusCode.Valid {
			continue
		}
		rvu.Source = source
		rvu.ExtractTime = extractTime
		rvu.LastModified = lastModified
		rvu.EffectiveDate = effectiveDate
		if err != nil {
			return nil, err
		}
		if err := rvu.Process(); err != nil {
			return nil, err
		}
		rvus = append(rvus, rvu)
	}

	return rvus, nil
}

func (r RelativeValueUnits) CreatePostgresTable(ctx context.Context, db *sqlx.DB, schema, table string) (sql.Result, error) {

	if schema == "" {
		schema = "cmsrvu"
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("create schema if not exists %s", schema))
	if err != nil {
		return nil, err
	}
	q := `
	create table if not exists %s.%s (
		_id_hash text primary key,
		_source text,         
		_extract_time timestamptz,   
		_last_modified timestamptz,  
		_effective_date date, 
		hcpcs text,
		modifier_code text,
		modifier text, 
		description text,
		status_code text,
		status text, 
		wrvu numeric,
		nonfacility_pervu numeric,
		nonfacility_na_indicator boolean,
		facility_pervu numeric,
		facility_na_indicator boolean,
		malpractice_rvu numeric,
		total_nonfacility_rvu numeric,
		total_facility_rvu numeric,
		pctc_indicator int,
		pctc text,
		global_surgery_code text,
		global_surgery text,
		preoperative_surgery numeric,
		intraoperative_surgery numeric,
		postoperative_surgery numeric,
		multiple_procedure_code int,
		multiple_procedure text,
		bilateral_surgery_code int,
		bilateral_surgery text,
		assistant_at_surgery_code int,
		assistant_at_surgery text,
		cosurgeons_code int,
		cosurgeons text,
		team_surgery_code int,
		team_surgery text,
		endoscopic_base_code text,
		conversion_factor numeric,
		physician_supervision_of_diagnostic_procedures_code text,
		physician_supervision_of_diagnostic_procedures text,
		calculation_flag int,
		diagnostic_imaging_family_indicator int,
		diagnostic_imaging_family text,
		nonfacility_pe_used_for_opps_payment_amount numeric,
		facility_pe_used_for_opps_payment_amount numeric,
		malpractice_used_for_opps_payment_amount numeric
	)`
	return db.ExecContext(ctx, fmt.Sprintf(q, schema, table))
}

func (r RelativeValueUnits) PutPostgres(db *sqlx.DB, schema, table string) (sql.Result, error) {
	q := `
	insert into %s.%s (
	_id_hash,
	_source,         
	_extract_time,   
	_last_modified,  
	_effective_date, 
	hcpcs,
	modifier_code,
	modifier, 
	description,
	status_code,
	status, 
	wrvu,
	nonfacility_pervu,
	nonfacility_na_indicator,
	facility_pervu,
	facility_na_indicator,
	malpractice_rvu,
	total_nonfacility_rvu,
	total_facility_rvu,
	pctc_indicator,
	pctc,
	global_surgery_code,
	global_surgery,
	preoperative_surgery,
	intraoperative_surgery,
	postoperative_surgery,
	multiple_procedure_code,
	multiple_procedure,
	bilateral_surgery_code,
	bilateral_surgery,
	assistant_at_surgery_code,
	assistant_at_surgery,
	cosurgeons_code,
	cosurgeons,
	team_surgery_code,
	team_surgery,
	endoscopic_base_code,
	conversion_factor,
	physician_supervision_of_diagnostic_procedures_code,
	physician_supervision_of_diagnostic_procedures,
	calculation_flag,
	diagnostic_imaging_family_indicator,
	diagnostic_imaging_family,
	nonfacility_pe_used_for_opps_payment_amount,
	facility_pe_used_for_opps_payment_amount,
	malpractice_used_for_opps_payment_amount
	) values (
	:_id_hash,
	:_source,         
	:_extract_time,   
	:_last_modified,  
	:_effective_date, 
	:hcpcs,
	:modifier_code,
	:modifier, 
	:description,
	:status_code,
	:status, 
	:wrvu,
	:nonfacility_pervu,
	:nonfacility_na_indicator,
	:facility_pervu,
	:facility_na_indicator,
	:malpractice_rvu,
	:total_nonfacility_rvu,
	:total_facility_rvu,
	:pctc_indicator,
	:pctc,
	:global_surgery_code,
	:global_surgery,
	:preoperative_surgery,
	:intraoperative_surgery,
	:postoperative_surgery,
	:multiple_procedure_code,
	:multiple_procedure,
	:bilateral_surgery_code,
	:bilateral_surgery,
	:assistant_at_surgery_code,
	:assistant_at_surgery,
	:cosurgeons_code,
	:cosurgeons,
	:team_surgery_code,
	:team_surgery,
	:endoscopic_base_code,
	:conversion_factor,
	:physician_supervision_of_diagnostic_procedures_code,
	:physician_supervision_of_diagnostic_procedures,
	:calculation_flag,
	:diagnostic_imaging_family_indicator,
	:diagnostic_imaging_family,
	:nonfacility_pe_used_for_opps_payment_amount,
	:facility_pe_used_for_opps_payment_amount,
	:malpractice_used_for_opps_payment_amount
	) on conflict (_id_hash) do nothing
	`
	return db.NamedExec(fmt.Sprintf(q, schema, table), r)

}
