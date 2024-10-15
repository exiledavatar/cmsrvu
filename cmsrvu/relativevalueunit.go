package cmsrvu

import (
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/exiledavatar/gotoolkit/meta"
	"github.com/jackc/pgx/v5/pgtype"
)

// RelativeValueUnit represents an expanded line from CMS's physician relative value files:
//   - meta data to indicate source and extract date
//   - effective date - this is defined by the CMS website/link and the accompanying pdf document
//   - many fields are renamed from X to XCode and an additional field added with the original X name -
//     this captures the original files code in the XCode field and a reasonable label from the pdf
//     document in the X field
type RelativeValueUnit struct {
	Source                                         string          `db:"_source"`                                                            // meta - should be the source url
	ExtractTime                                    time.Time       `db:"_extract_time"`                                                      // meta - attempts to capture actual extract (or get) time
	LastModified                                   time.Time       `db:"_last_modified"`                                                     // meta - taken from last-modified header in http response
	IDHash                                         string          `json:"_id_hash,omitempty" db:"_id_hash" pgtype:"text" primarykey:"true"` // hash of identifying fields
	EffectiveDate                                  pgtype.Date     `db:"_effective_date" idhash:"true"`                                      // added field
	HCPCS                                          string          `csv:"HCPCS" db:"hcpcs" idhash:"true"`
	ModifierCode                                   sql.NullString  `csv:"MOD" db:"modifier_code" idhash:"true"`
	Modifier                                       sql.NullString  `db:"modifier" idhash:"true"` // added field
	Description                                    sql.NullString  `csv:"DESCRIPTION" db:"description" idhash:"true"`
	StatusCode                                     sql.NullString  `csv:"STATUS CODE" db:"status_code" idhash:"true"`
	Status                                         sql.NullString  `db:"status" idhash:"true"` // added field
	NotUsedForMedicarePayment                      bool            `csv:"NOT USED FOR MEDICARE  PAYMENT" db:""`
	WRVU                                           sql.NullFloat64 `csv:"WORK RVU" db:"wrvu" idhash:"true"`
	NonFacilityPERVU                               sql.NullFloat64 `csv:"NON-FAC PE RVU" db:"nonfacility_pervu" idhash:"true"`
	NonFacilityNAIndicator                         bool            `csv:"NON-FAC NA INDICATOR" db:"nonfacility_na_indicator" idhash:"true"`
	FacilityPERVU                                  sql.NullFloat64 `csv:"FACILITY PE RVU" db:"facility_pervu" idhash:"true"`
	FacilityNAIndicator                            bool            `csv:"FACILITY  NA INDICATOR" db:"facility_na_indicator" idhash:"true"`
	MalpracticeRVU                                 sql.NullFloat64 `csv:"MP RVU" db:"malpractice_rvu" idhash:"true"`
	TotalNonFacilityRVU                            sql.NullFloat64 `csv:"NON-FACILITY TOTAL" db:"total_nonfacility_rvu" idhash:"true"`
	TotalFacilityRVU                               sql.NullFloat64 `csv:"FACILITY TOTAL" db:"total_facility_rvu" idhash:"true"`
	PCTCIndicator                                  sql.NullInt64   `csv:"PCTC IND" db:"pctc_indicator" idhash:"true"`
	PCTC                                           sql.NullString  `db:"pctc" idhash:"true"`
	GlobalSurgeryCode                              sql.NullString  `csv:"GLOB DAYS" db:"global_surgery_code" idhash:"true"`
	GlobalSurgery                                  sql.NullString  `db:"global_surgery" idhash:"true"`
	PreoperativePercentage                         sql.NullFloat64 `csv:"PRE OP" db:"preoperative_surgery" idhash:"true"`
	IntraoperativePercentage                       sql.NullFloat64 `csv:"INTRA OP" db:"intraoperative_surgery" idhash:"true"`
	PostoperativePercentage                        sql.NullFloat64 `csv:"POST OP" db:"postoperative_surgery" idhash:"true"`
	MultipleProcedureCode                          sql.NullInt64   `csv:"MULT PROC" db:"multiple_procedure_code" idhash:"true"`
	MultipleProcedure                              sql.NullString  `db:"multiple_procedure" idhash:"true"`
	BilateralSurgeryCode                           sql.NullInt64   `csv:"BILAT SURG" db:"bilateral_surgery_code" idhash:"true"`
	BilateralSurgery                               sql.NullString  `db:"bilateral_surgery" idhash:"true"`
	AssistantAtSurgeryCode                         sql.NullInt64   `csv:"ASST SURG" db:"assistant_at_surgery_code" idhash:"true"`
	AssistantAtSurgery                             sql.NullString  `db:"assistant_at_surgery" idhash:"true"`
	CoSurgeonsCode                                 sql.NullInt64   `csv:"CO-SURG" db:"cosurgeons_code" idhash:"true"`
	CoSurgeons                                     sql.NullString  `db:"cosurgeons" idhash:"true"`
	TeamSurgeryCode                                sql.NullInt64   `csv:"TEAM SURG" db:"team_surgery_code" idhash:"true"`
	TeamSurgery                                    sql.NullString  `db:"team_surgery" idhash:"true"`
	EndoscopicBaseCode                             sql.NullString  `csv:"ENDO BASE" db:"endoscopic_base_code" idhash:"true"`
	ConversionFactor                               sql.NullFloat64 `csv:"CONV FACTOR" db:"conversion_factor" idhash:"true"`
	PhysicianSupervisionOfDiagnosticProceduresCode sql.NullString  `csv:"PHYSICIAN SUPERVISION OF DIAGNOSTIC PROCEDURES" db:"physician_supervision_of_diagnostic_procedures_code" idhash:"true"`
	PhysicianSupervisionOfDiagnosticProcedures     sql.NullString  `db:"physician_supervision_of_diagnostic_procedures" idhash:"true"`
	CalculationFlag                                sql.NullInt64   `csv:"CALCULATION FLAG" db:"calculation_flag" idhash:"true"`
	DiagnosticImagingFamilyIndicator               sql.NullInt64   `csv:"DIAGNOSTIC IMAGING FAMILY INDICATOR" db:"diagnostic_imaging_family_indicator" idhash:"true"`
	DiagnosticImagingFamily                        sql.NullString  `db:"diagnostic_imaging_family" idhash:"true"`
	NonFacilityPEUsedForOppsPaymentAmount          sql.NullFloat64 `csv:"NON-FACILITY PE USED FOR OPPS PAYMENT AMOUNT" db:"nonfacility_pe_used_for_opps_payment_amount" idhash:"true"`
	FacilityPEUsedForOppsPaymentAmount             sql.NullFloat64 `csv:"FACILITY PE USED FOR OPPS PAYMENT AMOUNT" db:"facility_pe_used_for_opps_payment_amount" idhash:"true"`
	MalpracticeUsedForOppsPaymentAmount            sql.NullFloat64 `csv:"MP USED FOR OPPS PAYMENT AMOUNT" db:"malpractice_used_for_opps_payment_amount" idhash:"true"`
}

// func (*RelativeValueUnit) Unmarshal(data []byte)

func RVUFromRecord(in []string) (RelativeValueUnit, error) {
	errs := []error{}
	// fmt.Println("RVUFromRecord: Begin")
	// for i, e := range in[0:15] {
	// 	fmt.Println(i, len(e), e)
	// }
	// fmt.Printf("%#v\n", in)
	// process floats
	floats := map[int]sql.NullFloat64{}
	for _, fieldIndex := range []int{5, 6, 8, 10, 11, 12, 15, 16, 17, 24, 28, 29, 30} {
		floats[fieldIndex] = toSQLNullFloat64(in[fieldIndex])
	}

	// process ints
	ints := map[int]sql.NullInt64{}
	for _, fieldIndex := range []int{13, 18, 19, 20, 21, 22, 26, 27} {
		ints[fieldIndex] = toSQLNullInt64(in[fieldIndex])
	}

	// process strings
	strs := map[int]sql.NullString{}
	for _, fieldIndex := range []int{1, 2, 3, 14, 23, 25} {
		strs[fieldIndex] = toSQLNullString(in[fieldIndex])
	}

	rvu := RelativeValueUnit{
		HCPCS:                     cleanString(in[0]),
		ModifierCode:              strs[1],
		Modifier:                  ToModifier(strs[1]),
		Description:               strs[2],
		StatusCode:                strs[3],
		Status:                    ToStatus(strs[3]),
		NotUsedForMedicarePayment: cleanString(in[4]) != "",
		WRVU:                      floats[5],
		NonFacilityPERVU:          floats[6],
		NonFacilityNAIndicator:    cleanString(in[7]) == "NA",
		FacilityPERVU:             floats[8],
		FacilityNAIndicator:       cleanString(in[9]) == "NA",
		MalpracticeRVU:            floats[10],
		TotalNonFacilityRVU:       floats[11],
		TotalFacilityRVU:          floats[12],
		PCTCIndicator:             ints[13],
		PCTC:                      ToPCTC(ints[13]),
		//  if ints[13].Valid { ToPCTC(int(ints[13].Int64) } else "",
		// ToPCTC(int(ints[13].Int64)),
		GlobalSurgeryCode:        strs[14],
		GlobalSurgery:            ToGlobalSurgery(strs[14]),
		PreoperativePercentage:   floats[15],
		IntraoperativePercentage: floats[16],
		PostoperativePercentage:  floats[17],

		MultipleProcedureCode:  ints[18],
		MultipleProcedure:      ToMultipleProcedure(ints[18]),
		BilateralSurgeryCode:   ints[19],
		BilateralSurgery:       ToBilateralSurgery(ints[19]),
		AssistantAtSurgeryCode: ints[20],
		AssistantAtSurgery:     ToAssistantAtSurgery(ints[20]),
		CoSurgeonsCode:         ints[21],
		CoSurgeons:             ToCosurgeons(ints[21]),
		TeamSurgeryCode:        ints[22],
		TeamSurgery:            ToTeamSurgery(ints[22]),
		EndoscopicBaseCode:     strs[23],
		ConversionFactor:       floats[24],
		PhysicianSupervisionOfDiagnosticProceduresCode: strs[25],
		PhysicianSupervisionOfDiagnosticProcedures:     ToPhysicianSupervisionOfDiagnosticProcedures(strs[25]),
		CalculationFlag:                       ints[26],
		DiagnosticImagingFamilyIndicator:      ints[27],
		DiagnosticImagingFamily:               ToDiagnosticImagingFamily(ints[27]),
		NonFacilityPEUsedForOppsPaymentAmount: floats[28],
		FacilityPEUsedForOppsPaymentAmount:    floats[29],
		MalpracticeUsedForOppsPaymentAmount:   floats[30],
	}

	return rvu, errors.Join(errs...)
}

func (r *RelativeValueUnit) SetIDHash() error {
	if r.EffectiveDate.Time.IsZero() {
		return errors.New("EffectiveDate cannot be zero")
	}

	idh := meta.ToValueMap(*r, "idhash").Hash()
	r.IDHash = idh
	return nil
}

func (r *RelativeValueUnit) Process() error {
	return r.SetIDHash()
}

func cleanString(s string) string {
	s = strings.ToValidUTF8(s, "")
	return strings.TrimSpace(s)
}

func toSQLNullString(s string) sql.NullString {
	s = cleanString(s)
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

var floatRegex = regexp.MustCompile(`[^\d.]+`)

func toSQLNullFloat64(s string) sql.NullFloat64 {
	cleaned := cleanString(s)
	cleaned = floatRegex.ReplaceAllString(cleaned, "")
	switch out, err := strconv.ParseFloat(cleaned, 64); {
	case cleaned != "" && err == nil:
		return sql.NullFloat64{
			Float64: out,
			Valid:   true,
		}
	default:
		return sql.NullFloat64{
			Float64: 0.0,
			Valid:   false,
		}
	}
}

var intRegex = regexp.MustCompile(`[^\d]+`)

func toSQLNullInt64(s string) sql.NullInt64 {
	cleaned := cleanString(s)
	cleaned = intRegex.ReplaceAllString(cleaned, "")
	switch out, err := strconv.ParseInt(cleaned, 10, 64); {
	case cleaned != "" && err == nil:
		return sql.NullInt64{
			Int64: out,
			Valid: true,
		}
	default:
		return sql.NullInt64{
			Int64: 0,
			Valid: false,
		}
	}
}

func ToModifier(code sql.NullString) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.String {
	case "26":
		return toSQLNullString("Professional Component")
	case "TC":
		return toSQLNullString("Technical Component")
	case "53":
		return toSQLNullString("Discontinued Procedure")
	default:
		return toSQLNullString("")
	}
}

func ToStatus(code sql.NullString) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.String {
	case "A":
		return toSQLNullString("Active")
	case "B":
		return toSQLNullString("Bundled")
	case "C":
		return toSQLNullString("Contractors Price the Code")
	case "D":
		return toSQLNullString("Deleted")
	case "E":
		return toSQLNullString("Excluded from PFS by Regulation")
	case "F":
		return toSQLNullString("Deleted/Discontinued (no grace period)")
	case "G":
		return toSQLNullString("Not Valid for Medicare")
	case "H":
		return toSQLNullString("Deleted Modifier")
	case "I":
		return toSQLNullString("Not Valid for Medicare (no grace period)")
	case "J":
		return toSQLNullString("Anesthesia Services")
	case "M":
		return toSQLNullString("Measurement - For Reporting Purposes Only")
	case "N":
		return toSQLNullString("Non-Covered Services")
	case "P":
		return toSQLNullString("Bundled/Excluded")
	case "R":
		return toSQLNullString("Restricted Coverage")
	case "T":
		return toSQLNullString("Injections")
	case "X":
		return toSQLNullString("Statutory Exclusion")
	default:
		return toSQLNullString("")
	}
}

func ToPCTC(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("Physician Service")
	case 1:
		return toSQLNullString("Diagnostic Tests for Radiology Services")
	case 2:
		return toSQLNullString("Professional Component Only")
	case 3:
		return toSQLNullString("Technical Component Only")
	case 4:
		return toSQLNullString("Global Test Only")
	case 5:
		return toSQLNullString("Incident To")
	case 6:
		return toSQLNullString("Laboratory Physician Interpretation")
	case 7:
		return toSQLNullString("Physical Therapy Service")
	case 8:
		return toSQLNullString("Physician Interpretation")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToGlobalSurgery(code sql.NullString) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.String {
	case "0":
		fallthrough
	case "000":
		return toSQLNullString("Endoscopic/Minor: includes 1 day preoperative, 1 day postoperative, excludes evaluation and management")
	case "10":
		fallthrough
	case "010":
		return toSQLNullString("Minor: includes 1 day preoperative, 10 day postoperative")
	case "90":
		fallthrough
	case "090":
		return toSQLNullString("Major: includes 1 day preoperative, 90 day postoperative")
	case "MMM":
		return toSQLNullString("Maternity: global period does not apply")
	case "XXX":
		return toSQLNullString("Not Applicable")
	case "YYY":
		return toSQLNullString("Determined by Carrier")
	case "ZZZ":
		return toSQLNullString("Part of Another Service")
	default:
		return toSQLNullString("")
	}
}

func ToMultipleProcedure(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("No Adjustment")
	case 1:
		return toSQLNullString("Standard Adjustment Rank 1")
	case 2:
		return toSQLNullString("Standard Adjustment Rank 2")
	case 3:
		return toSQLNullString("Group by Endoscopic Base Code")
	case 4:
		return toSQLNullString("Group by Diagnostic Imaging Code")
	case 5:
		return toSQLNullString("Therapy Service - 50% Practice Expense")
	case 6:
		return toSQLNullString("Diagnostic Cardiovascular Service - 25% Reduction to non-maximum and subsequent")
	case 7:
		return toSQLNullString("Diagnostic Ophthalmology Service - 20% Reduction to non-maximum and subsequent")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToBilateralSurgery(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("Bilateral Adjustment Does Not Apply - See CMS Documents for Details")
	case 1:
		return toSQLNullString("150% Bilateral Adjustment")
	case 2:
		return toSQLNullString("Bilateral Adjustment Does Not Apply - See CMS Documents for Details")
	case 3:
		return toSQLNullString("Bilateral Adjustment Does Not Apply - See CMS Documents for Details")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToAssistantAtSurgery(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("Proof of Medical Necessity Required for Assistants at Surgery")
	case 1:
		return toSQLNullString("Statutory Payment Restriction for Assistants at Surgery")
	case 2:
		return toSQLNullString("No Payment Restriction for Assistants at Surgery")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToCosurgeons(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("Co-surgeons Not Permitted")
	case 1:
		return toSQLNullString("Proof of Medical Necessity Required for Co-surgeons")
	case 2:
		return toSQLNullString("Co-surgeons Permitted")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToTeamSurgery(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 0:
		return toSQLNullString("Team Surgeons Not Permitted")
	case 1:
		return toSQLNullString("Proof of Medical Necessity Required for Team Surgeons")
	case 2:
		return toSQLNullString("Team Surgeons Permitted")
	case 9:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToPhysicianSupervisionOfDiagnosticProcedures(code sql.NullString) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.String {
	case "1":
		fallthrough
	case "01":
		return toSQLNullString("General Supervision Required")
	case "2":
		fallthrough
	case "02":
		return toSQLNullString("Direct Supervision Required")
	case "3":
		fallthrough
	case "03":
		return toSQLNullString("Personal Supervision Required")
	case "4":
		fallthrough
	case "04":
		return toSQLNullString("Not Required for Psychologist - Otherwise General Supervision Required")
	case "5":
		fallthrough
	case "05":
		return toSQLNullString("Not Required for Audiologist - Otherwise General Supervision Required")
	case "6":
		fallthrough
	case "06":
		return toSQLNullString("Must Be Performed by ABPTS Electrophysiological Specialist PT or Physician")
	case "21":
		return toSQLNullString("General Required for Technician - Otherwise Direct Supervision Required")
	case "22":
		return toSQLNullString("May Be Performed by Technician with Online Real-Time Contact with Physician")
	case "66":
		return toSQLNullString("May Be Performed by Physician or PT with Appropriate ABPTS Certification")
	case "6A":
		return toSQLNullString("Extension of Code 66 - Additionally Certified PT may Supervise Another PT")
	case "77":
		return toSQLNullString("May Be Performed by: PT with ABPTS Certification, PT Under Direct Physician Supervision, Technician with Certification under General Supervision")
	case "7A":
		return toSQLNullString("Extension of Code 77 - Additionally Certified PT may Supervise Another PT")
	case "09":
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}

func ToDiagnosticImagingFamily(code sql.NullInt64) sql.NullString {
	if !code.Valid {
		return toSQLNullString("")
	}
	switch code.Int64 {
	case 1:
		return toSQLNullString("Ultrasound (Chest/Abdomen/Pelvis-Non-Obstetrical)")
	case 2:
		return toSQLNullString("CT and CTA (Chest/Thorax/Abd/Pelvis)")
	case 3:
		return toSQLNullString("CT and CTA (Head/Brain/Orbit/Maxillofacial/Neck)")
	case 4:
		return toSQLNullString("MRI and MRA (Chest/Abd/Pelvis)")
	case 5:
		return toSQLNullString("MRI and MRA (Head/Brain/Neck)")
	case 6:
		return toSQLNullString("MRI and MRA (Spine)")
	case 7:
		return toSQLNullString("CT (Spine)")
	case 8:
		return toSQLNullString("MRI and MRA (Lower Extremities)")
	case 9:
		return toSQLNullString("CT and CTA (Lower Extremities)")
	case 10:
		return toSQLNullString("MR and MRI (Upper Extremities and Joints)")
	case 11:
		return toSQLNullString("CT and CTA (Upper Extremities)")
	case 88:
		return toSQLNullString("Subject to Reduction of TC after 2011-01-01 and PC 2012-01-01")
	case 99:
		return toSQLNullString("Not Applicable")
	default:
		return toSQLNullString("")
	}
}
