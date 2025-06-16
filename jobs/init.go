package main

import (
	"log/slog"
	"os"

	"github.com/case-framework/case-backend/pkg/db"
	userDB "github.com/case-framework/case-backend/pkg/db/participant-user"
	studyDB "github.com/case-framework/case-backend/pkg/db/study"
	"github.com/case-framework/case-backend/pkg/utils"
	"gopkg.in/yaml.v2"
)

// Environment variables
const (
	ENV_CONFIG_FILE_PATH = "CONFIG_FILE_PATH"

	// Variables to override "secrets" in the config file
	ENV_STUDY_DB_USERNAME            = "STUDY_DB_USERNAME"
	ENV_STUDY_DB_PASSWORD            = "STUDY_DB_PASSWORD"
	ENV_PARTICIPANT_USER_DB_USERNAME = "PARTICIPANT_USER_DB_USERNAME"
	ENV_PARTICIPANT_USER_DB_PASSWORD = "PARTICIPANT_USER_DB_PASSWORD"
)

type config struct {
	// Logging configs
	Logging utils.LoggerConfig `json:"logging" yaml:"logging"`

	// DB configs
	DBConfigs struct {
		ParticipantUserDB db.DBConfigYaml `json:"participant_user_db" yaml:"participant_user_db"`
		StudyDB           db.DBConfigYaml `json:"study_db" yaml:"study_db"`
	} `json:"db_configs" yaml:"db_configs"`

	InstanceID string `json:"instance_id" yaml:"instance_id"`
}

var conf config

var (
	participantUserDBService *userDB.ParticipantUserDBService
	studyDBService           *studyDB.StudyDBService
)

func init() {
	// Read config from file
	yamlFile, err := os.ReadFile(os.Getenv(ENV_CONFIG_FILE_PATH))
	if err != nil {
		panic(err)
	}

	err = yaml.UnmarshalStrict(yamlFile, &conf)
	if err != nil {
		panic(err)
	}

	// Init logger:
	utils.InitLogger(
		conf.Logging.LogLevel,
		conf.Logging.IncludeSrc,
		conf.Logging.LogToFile,
		conf.Logging.Filename,
		conf.Logging.MaxSize,
		conf.Logging.MaxAge,
		conf.Logging.MaxBackups,
		conf.Logging.CompressOldLogs,
		conf.Logging.IncludeBuildInfo,
	)

	initDBs()

}

func initDBs() {
	var err error
	participantUserDBService, err = userDB.NewParticipantUserDBService(db.DBConfigFromYamlObj(conf.DBConfigs.ParticipantUserDB, []string{conf.InstanceID}))
	if err != nil {
		slog.Error("Error connecting to Participant User DB", slog.String("error", err.Error()))
		panic(err)
	}

	studyDBService, err = studyDB.NewStudyDBService(db.DBConfigFromYamlObj(conf.DBConfigs.StudyDB, []string{conf.InstanceID}))
	if err != nil {
		slog.Error("Error connecting to Study DB", slog.String("error", err.Error()))
		panic(err)
	}
}
