package main

import (
	"log/slog"
	"regexp"
	"strings"

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
		FirstName:  row.FirstName,
		LastName:   row.LastName,
		City:       row.City,
		PostalCode: row.ZipCode,
	}

	// Trim whitespace
	address := strings.TrimSpace(row.Address)

	if address == "" {
		return result
	}

	// Regex pattern for Dutch addresses
	// ^(.+?)\s+(\d+)([a-zA-Z]*)(?:\s*[-\s]\s*(.+))?$
	pattern := `^(.+?)\s+(\d+)([a-zA-Z]*)(?:\s*[-\s]\s*(.+))?$`
	regex := regexp.MustCompile(pattern)

	matches := regex.FindStringSubmatch(address)
	if matches == nil {
		return result
	}

	result.Street = strings.TrimSpace(matches[1])
	result.HouseNumber = matches[2] + matches[3] // number + suffix

	// Add extra info if present (matches[4])
	if len(matches) > 4 && matches[4] != "" {
		result.Street2 = strings.TrimSpace(matches[4])
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
	address := prepAddress(row)

	slog.Debug("Address", slog.Any("address", address))

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

	// TODO: Update address

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
