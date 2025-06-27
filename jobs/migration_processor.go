package main

import (
	"log/slog"

	studyTypes "github.com/case-framework/case-backend/pkg/study/types"
	studyUtils "github.com/case-framework/case-backend/pkg/study/utils"
	userTypes "github.com/case-framework/case-backend/pkg/user-management/types"
)

// ExampleProcessor demonstrates how to implement CSVProcessor interface
type MigrateDataProcessor struct {
	processedCount int
}

func prepPhoneNumber(row CSVRow) string {
	if row.PhoneNumber == "" {
		return ""
	}

	if len(row.PhoneNumber) > 0 && row.PhoneNumber[0] != '+' {
		return "+" + row.PhoneNumber
	}

	return row.PhoneNumber
}

type Address struct {
	FirstName   string
	LastName    string
	Street      string
	HouseNumber string
	Street2     string
	City        string
	PostalCode  string
	Country     string
}

func prepAddress(row CSVRow) Address {
	result := Address{
		FirstName:   row.FirstName,
		LastName:    row.LastName,
		Street:      row.Street,
		HouseNumber: row.HouseNumber,
		Street2:     row.HouseExt,
		City:        row.City,
		PostalCode:  row.ZipCode,
	}

	return result
}

func (p *MigrateDataProcessor) ProcessRow(row CSVRow, rowIndex int) bool {
	slog.Debug("Processing row",
		slog.Int("index", rowIndex),
		slog.String("pid", row.ParticipantID),
	)

	p.processedCount++

	if row.ParticipantID == "" {
		slog.Error("Missing required field 'dn_extra_usn2' in row", slog.Int("row_index", rowIndex))
		return false
	}

	phoneNumber := prepPhoneNumber(row)

	// Check if participant exists
	if !participantExistsAndActive(conf.InstanceID, conf.StudyKey, row.ParticipantID) {
		slog.Error("Participant does not exist or account was deleted", slog.String("pid", row.ParticipantID), slog.String("instance_id", conf.InstanceID), slog.String("study_key", conf.StudyKey), slog.Int("row_index", rowIndex))
		return false
	}

	// Compute confidential ID
	confidentialID, err := studyUtils.ProfileIDtoParticipantID(row.ParticipantID, conf.StudyGlobalSecret, studySecretKey, studyIdMappingMethod)
	if err != nil {
		slog.Error("Error computing participant IDs", slog.String("instanceID", conf.InstanceID), slog.String("studyKey", conf.StudyKey), slog.String("error", err.Error()))
		return false
	}

	// Get confidential ID map
	profileID, err := studyDBService.GetProfileIDFromConfidentialID(conf.InstanceID, confidentialID, conf.StudyKey)
	if err != nil {
		slog.Error("Error getting profile ID from confidential ID", slog.String("instanceID", conf.InstanceID), slog.String("studyKey", conf.StudyKey), slog.String("error", err.Error()))
		return false
	}

	// Find user by profile ID
	user, err := participantUserDBService.GetUserByProfileID(conf.InstanceID, profileID)
	if err != nil {
		slog.Error("Error getting user by profile ID", slog.String("instanceID", conf.InstanceID), slog.String("studyKey", conf.StudyKey), slog.String("error", err.Error()))
		return false
	}

	// Update phone number
	shouldUpdatePhoneNumber := false
	if hasPhoneNumber(user) {
		if conf.ForceOverridePhone {
			user.SetPhoneNumber(phoneNumber)
			_ = user.ConfirmPhoneNumber()
			slog.Info("Overriding phone number for user", slog.String("user_id", user.ID.Hex()))
			shouldUpdatePhoneNumber = true
		} else {
			slog.Info("Phone number already exists for user, use the config 'force_override_phone' to override it", slog.String("user_id", user.ID.Hex()))
		}
	} else {
		user.SetPhoneNumber(phoneNumber)
		_ = user.ConfirmPhoneNumber()
		shouldUpdatePhoneNumber = true
	}
	if shouldUpdatePhoneNumber {
		if conf.DryRun {
			slog.Info("Would update phone number for user", slog.String("user_id", user.ID.Hex()))
		} else {
			if _, err = participantUserDBService.ReplaceUser(conf.InstanceID, user); err != nil {
				slog.Error("Error updating phone number for user", slog.String("instanceID", conf.InstanceID), slog.String("studyKey", conf.StudyKey), slog.String("error", err.Error()))
				return false
			}
			slog.Info("Updated phone number for user", slog.String("user_id", user.ID.Hex()))
		}
	}

	// Update address
	contactInfo := prepNewContactInfoResponse(row, confidentialID)
	if conf.DryRun {
		slog.Info("Would update address for user", slog.String("user_id", user.ID.Hex()), slog.Any("contact_info", contactInfo))
	} else {
		err := studyDBService.ReplaceConfidentialResponse(conf.InstanceID, conf.StudyKey, contactInfo)
		if err != nil {
			slog.Error("Unexpected error while updating contact info", slog.String("instanceID", conf.InstanceID), slog.String("studyKey", conf.StudyKey), slog.String("error", err.Error()))
			return false
		}
		slog.Info("Updated address for user", slog.String("user_id", user.ID.Hex()))
	}

	return true
}

func participantExistsAndActive(instanceID string, studyKey string, pid string) bool {
	p, err := studyDBService.GetParticipantByID(instanceID, studyKey, pid)
	if err != nil {
		slog.Error("Error getting participant", slog.String("error", err.Error()))
		return false
	}

	return p.StudyStatus == "active"
}

func hasPhoneNumber(user userTypes.User) bool {
	contactInfo, err := user.GetPhoneNumber()
	if err != nil {
		return false
	}

	return contactInfo.Phone != ""
}

func prepNewContactInfoResponse(row CSVRow, confidentialID string) studyTypes.SurveyResponse {
	address := prepAddress(row)

	rItem := studyTypes.SurveyResponse{
		Key:           "SwabEntry.Addr",
		ParticipantID: confidentialID,
		Responses: []studyTypes.SurveyItemResponse{
			{
				Key: "SwabEntry.Addr",
				Response: &studyTypes.ResponseItem{
					Key: "rg",
					Items: []*studyTypes.ResponseItem{
						{
							Key: "contact",
							Items: []*studyTypes.ResponseItem{
								{
									Key:   "fullName",
									Value: "",
								},
								{
									Key:   "givenName",
									Value: address.FirstName,
								},
								{
									Key:   "familyName",
									Value: address.LastName,
								},
								{
									Key:   "company",
									Value: "",
								},
								{
									Key:   "email",
									Value: "",
								},
								{
									Key:   "phone",
									Value: "",
								},
								{
									Key:   "street",
									Value: address.Street,
								},
								{
									Key:   "street2",
									Value: address.Street2,
								},
								{
									Key:   "city",
									Value: address.City,
								},
								{
									Key:   "postalCode",
									Value: address.PostalCode,
								},
								{
									Key:   "country",
									Value: address.Country,
								},
								{
									Key:   "houseNumber",
									Value: address.HouseNumber,
								},
							},
						},
					},
				},
			},
		},
	}

	return rItem
}
