package cmsrvu

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

// RelativeValueUnit represents an expanded line from CMS's physician relative value files:
//   - meta data to indicate source and extract date
//   - effective date - this is defined by the CMS website/link and the accompanying pdf document
//   - many fields are renamed from X to XCode and an additional field added with the original X name -
//     this captures the original files code in the XCode field and a reasonable label from the pdf
//     document in the X field
type RelativeValueUnit struct {
	Source                                         string         `db:"_source"`         // meta
	ExtractTime                                    time.Time      `db:"_extract_time"`   // meta
	EffectiveDate                                  time.Time      `db:"_effective_date"` // added field
	HCPCS                                          string         `csv:"HCPCS" db:"hcpcs"`
	ModifierCode                                   string         `csv:"MOD" db:"modifier_code"`
	Modifier                                       string         `db:"modifier"` // added field
	Description                                    string         `csv:"DESCRIPTION" db:"description"`
	StatusCode                                     string         `csv:"STATUS CODE" db:"status_code"`
	Status                                         string         `db:"status"` // added field
	NotUsedForMedicarePayment                      bool           `csv:"NOT USED FOR MEDICARE  PAYMENT" db:""`
	WRVU                                           float64        `csv:"WORK RVU" db:"wrvu"`
	NonFacilityPERVU                               float64        `csv:"NON-FAC PE RVU" db:"nonfacility_pervu"`
	NonFacilityNAIndicator                         bool           `csv:"NON-FAC NA INDICATOR" db:"nonfacility_na_indicator"`
	FacilityPERVU                                  float64        `csv:"FACILITY PE RVU" db:"facility_pervu"`
	FacilityNAIndicator                            bool           `csv:"FACILITY  NA INDICATOR" db:"facility_na_indicator"`
	MalpracticeRVU                                 float64        `csv:"MP RVU" db:"malpractice_rvu"`
	TotalNonFacilityRVU                            float64        `csv:"NON-FACILITY TOTAL" db:"total_nonfacility_rvu"`
	TotalFacilityRVU                               float64        `csv:"FACILITY TOTAL" db:"total_facility_rvu"`
	PCTCIndicator                                  int            `csv:"PCTC IND" db:"pctc_indicator"`
	PCTC                                           string         `db:"pctc"`
	GlobalSurgeryCode                              string         `csv:"GLOB DAYS" db:"global_surgery_code"`
	GlobalSurgery                                  string         `db:"global_surgery"`
	PreoperativePercentage                         float64        `csv:"PRE OP" db:"preoperative_surgery"`
	IntraoperativePercentage                       float64        `csv:"INTRA OP" db:"intraoperative_surgery"`
	PostoperativePercentage                        float64        `csv:"POST OP" db:"postoperative_surgery"`
	MultipleProcedureCode                          int            `csv:"MULT PROC" db:"multiple_procedure_code"`
	MultipleProcedure                              string         `db:"multiple_procedure"`
	BilateralSurgeryCode                           int            `csv:"BILAT SURG" db:"bilateral_surgery_code"`
	BilateralSurgery                               string         `db:"bilateral_surgery"`
	AssistantAtSurgeryCode                         int            `csv:"ASST SURG" db:"assistant_at_surgery_code"`
	AssistantAtSurgery                             string         `db:"assistant_at_surgery"`
	CoSurgeonsCode                                 int            `csv:"CO-SURG" db:"cosurgeons_code"`
	CoSurgeons                                     string         `db:"cosurgeons"`
	TeamSurgeryCode                                int            `csv:"TEAM SURG" db:"team_surgery_code"`
	TeamSurgery                                    string         `db:"team_surgery"`
	EndoscopicBaseCode                             sql.NullString `csv:"ENDO BASE" db:"endoscopic_base_code"`
	ConversionFactor                               float64        `csv:"CONV FACTOR" db:"conversion_factor"`
	PhysicianSupervisionOfDiagnosticProceduresCode string         `csv:"PHYSICIAN SUPERVISION OF DIAGNOSTIC PROCEDURES" db:"physician_supervision_of_diagnostic_procedures_code"`
	PhysicianSupervisionOfDiagnosticProcedures     string         `db:"physician_supervision_of_diagnostic_procedures"`
	CalculationFlag                                int            `csv:"CALCULATION FLAG" db:"calculation_flag"`
	DiagnosticImagingFamilyIndicator               int            `csv:"DIAGNOSTIC IMAGING FAMILY INDICATOR" db:"diagnostic_imaging_family_indicator"`
	DiagnosticImagingFamily                        string         `db:"diagnostic_imaging_family"`
	NonFacilityPEUsedForOppsPaymentAmount          float64        `csv:"NON-FACILITY PE USED FOR OPPS PAYMENT AMOUNT" db:"nonfacility_pe_used_for_opps_payment_amount"`
	FacilityPEUsedForOppsPaymentAmount             float64        `csv:"FACILITY PE USED FOR OPPS PAYMENT AMOUNT" db:"facility_pe_used_for_opps_payment_amount"`
	MalpracticeUsedForOppsPaymentAmount            float64        `csv:"MP USED FOR OPPS PAYMENT AMOUNT" db:"malpractice_used_for_opps_payment_amount"`
}

// func (*RelativeValueUnit) Unmarshal(data []byte)

func (r *RelativeValueUnit) FromStringSlice(in []string) error {

	errs := []error{}
	// process floats
	floats := map[int]float64{}
	for _, fieldIndex := range []int{5, 6, 8, 10, 11, 12, 15, 16, 17, 24, 28, 29, 30} {
		var err error
		trimmedValue := strings.TrimSpace(in[fieldIndex])
		floats[fieldIndex], err = strconv.ParseFloat(trimmedValue, 64)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// process ints
	ints := map[int]int{}
	for _, fieldIndex := range []int{13, 18, 19, 20, 21, 22, 26, 27} {
		var err error
		trimmedValue := strings.TrimSpace(in[fieldIndex])
		ints[fieldIndex], err = strconv.Atoi(trimmedValue)
		if err != nil {
			errs = append(errs, err)
		}
	}

	rvu := RelativeValueUnit{
		HCPCS:                     in[0],
		ModifierCode:              in[1],
		Modifier:                  ToModifier(in[1]),
		Description:               strings.ToValidUTF8(in[2], ""),
		StatusCode:                in[3],
		Status:                    ToStatus(in[3]),
		NotUsedForMedicarePayment: strings.TrimSpace(in[4]) != "",
		WRVU:                      floats[5],
		NonFacilityPERVU:          floats[6],
		NonFacilityNAIndicator:    strings.TrimSpace(in[7]) == "NA",
		FacilityPERVU:             floats[8],
		FacilityNAIndicator:       strings.TrimSpace(in[9]) == "NA",
		MalpracticeRVU:            floats[10],
		TotalNonFacilityRVU:       floats[11],
		TotalFacilityRVU:          floats[12],
		PCTCIndicator:             ints[13],
		PCTC:                      ToPCTC(ints[13]),
		GlobalSurgeryCode:         strings.TrimSpace(in[14]),
		GlobalSurgery:             ToGlobalSurgery(in[14]),
		PreoperativePercentage:    floats[15],
		IntraoperativePercentage:  floats[16],
		PostoperativePercentage:   floats[17],

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
		EndoscopicBaseCode: sql.NullString{
			String: strings.TrimSpace(in[23]),
			Valid:  true, // not done,
		},
		ConversionFactor: floats[24],
		PhysicianSupervisionOfDiagnosticProceduresCode: strings.TrimSpace(in[25]),
		PhysicianSupervisionOfDiagnosticProcedures:     ToPhysicianSupervisionOfDiagnosticProcedures(in[25]),
		CalculationFlag:                       ints[26],
		DiagnosticImagingFamilyIndicator:      ints[27],
		DiagnosticImagingFamily:               ToDiagnosticImagingFamily(ints[27]),
		NonFacilityPEUsedForOppsPaymentAmount: floats[28],
		FacilityPEUsedForOppsPaymentAmount:    floats[29],
		MalpracticeUsedForOppsPaymentAmount:   floats[30],
	}

	*r = rvu
	return errors.Join(errs...)
}

func toSQLNullString(s string) sql.NullString {
	s = strings.ToValidUTF8(s, "")
	s = strings.TrimSpace(s)
	return sql.NullString{
		String: s,
		Valid:  s != "",
	}
}

func ToModifier(code string) string {
	code = strings.ReplaceAll(code, "-", "")
	code = strings.TrimSpace(code)
	switch code {
	case "26":
		return "Professional Component"
	case "TC":
		return "Technical Component"
	case "53":
		return "Discontinued Procedure"
	default:
		return ""
	}
}

func ToStatus(code string) string {
	code = strings.ReplaceAll(code, "-", "")
	code = strings.TrimSpace(code)
	switch code {
	case "A":
		return "Active"
	case "B":
		return "Bundled"
	case "C":
		return "Contractors Price the Code"
	case "D":
		return "Deleted"
	case "E":
		return "Excluded from PFS by Regulation"
	case "F":
		return "Deleted/Discontinued (no grace period)"
	case "G":
		return "Not Valid for Medicare"
	case "H":
		return "Deleted Modifier"
	case "I":
		return "Not Valid for Medicare (no grace period)"
	case "J":
		return "Anesthesia Services"
	case "M":
		return "Measurement - For Reporting Purposes Only"
	case "N":
		return "Non-Covered Services"
	case "P":
		return "Bundled/Excluded"
	case "R":
		return "Restricted Coverage"
	case "T":
		return "Injections"
	case "X":
		return "Statutory Exclusion"
	default:
		return ""
	}
}

func ToPCTC(code int) string {
	switch code {
	case 0:
		return "Physician Service"
	case 1:
		return "Diagnostic Tests for Radiology Services"
	case 2:
		return "Professional Component Only"
	case 3:
		return "Technical Component Only"
	case 4:
		return "Global Test Only"
	case 5:
		return "Incident To"
	case 6:
		return "Laboratory Physician Interpretation"
	case 7:
		return "Physical Therapy Service"
	case 8:
		return "Physician Interpretation"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToGlobalSurgery(code string) string {
	code = strings.ReplaceAll(code, "-", "")
	code = strings.TrimSpace(code)
	switch code {
	case "000":
		return "Endoscopic/Minor: includes 1 day preoperative, 1 day postoperative, excludes evaluation and management"
	case "010":
		return "Minor: includes 1 day preoperative, 10 day postoperative"
	case "090":
		return "Major: includes 1 day preoperative, 90 day postoperative"
	case "MMM":
		return "Maternity: global period does not apply"
	case "XXX":
		return "Not Applicable"
	case "YYY":
		return "Determined by Carrier"
	case "ZZZ":
		return "Part of Another Service"
	default:
		return ""
	}
}

func ToMultipleProcedure(code int) string {
	switch code {
	case 0:
		return "No Adjustment"
	case 1:
		return "Standard Adjustment Rank 1"
	case 2:
		return "Standard Adjustment Rank 2"
	case 3:
		return "Group by Endoscopic Base Code"
	case 4:
		return "Group by Diagnostic Imaging Code"
	case 5:
		return "Therapy Service - 50% Practice Expense"
	case 6:
		return "Diagnostic Cardiovascular Service - 25% Reduction to non-maximum and subsequent"
	case 7:
		return "Diagnostic Ophthalmology Service - 20% Reduction to non-maximum and subsequent"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToBilateralSurgery(code int) string {
	switch code {
	case 0:
		return "Bilateral Adjustment Does Not Apply - See CMS Documents for Details"
	case 1:
		return "150% Bilateral Adjustment"
	case 2:
		return "Bilateral Adjustment Does Not Apply - See CMS Documents for Details"
	case 3:
		return "Bilateral Adjustment Does Not Apply - See CMS Documents for Details"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToAssistantAtSurgery(code int) string {
	switch code {
	case 0:
		return "Proof of Medical Necessity Required for Assistants at Surgery"
	case 1:
		return "Statutory Payment Restriction for Assistants at Surgery"
	case 2:
		return "No Payment Restriction for Assistants at Surgery"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToCosurgeons(code int) string {
	switch code {
	case 0:
		return "Co-surgeons Not Permitted"
	case 1:
		return "Proof of Medical Necessity Required for Co-surgeons"
	case 2:
		return "Co-surgeons Permitted"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToTeamSurgery(code int) string {
	switch code {
	case 0:
		return "Team Surgeons Not Permitted"
	case 1:
		return "Proof of Medical Necessity Required for Team Surgeons"
	case 2:
		return "Team Surgeons Permitted"
	case 9:
		return "Not Applicable"
	default:
		return ""
	}
}

func ToPhysicianSupervisionOfDiagnosticProcedures(code string) string {
	code = strings.ReplaceAll(code, "-", "")
	code = strings.TrimSpace(code)
	switch code {
	case "01":
		return "General Supervision Required"
	case "02":
		return "Direct Supervision Required"
	case "03":
		return "Personal Supervision Required"
	case "04":
		return "Not Required for Psychologist - Otherwise General Supervision Required"
	case "05":
		return "Not Required for Audiologist - Otherwise General Supervision Required"
	case "06":
		return "Must Be Performed by ABPTS Electrophysiological Specialist PT or Physician"
	case "21":
		return "General Required for Technician - Otherwise Direct Supervision Required"
	case "22":
		return "May Be Performed by Technician with Online Real-Time Contact with Physician"
	case "66":
		return "May Be Performed by Physician or PT with Appropriate ABPTS Certification"
	case "6A":
		return "Extension of Code 66 - Additionally Certified PT may Supervise Another PT"
	case "77":
		return "May Be Performed by: PT with ABPTS Certification, PT Under Direct Physician Supervision, Technician with Certification under General Supervision"
	case "7A":
		return "Extension of Code 77 - Additionally Certified PT may Supervise Another PT"
	case "09":
		return "Not Applicable"
	default:
		return ""
	}
}

func ToDiagnosticImagingFamily(code int) string {
	switch code {
	case 1:
		return "Ultrasound (Chest/Abdomen/Pelvis-Non-Obstetrical)"
	case 2:
		return "CT and CTA (Chest/Thorax/Abd/Pelvis)"
	case 3:
		return "CT and CTA (Head/Brain/Orbit/Maxillofacial/Neck)"
	case 4:
		return "MRI and MRA (Chest/Abd/Pelvis)"
	case 5:
		return "MRI and MRA (Head/Brain/Neck)"
	case 6:
		return "MRI and MRA (Spine)"
	case 7:
		return "CT (Spine)"
	case 8:
		return "MRI and MRA (Lower Extremities)"
	case 9:
		return "CT and CTA (Lower Extremities)"
	case 10:
		return "MR and MRI (Upper Extremities and Joints)"
	case 11:
		return "CT and CTA (Upper Extremities)"
	case 88:
		return "Subject to Reduction of TC after 2011-01-01 and PC 2012-01-01"
	case 99:
		return "Not Applicable"
	default:
		return ""
	}
}
