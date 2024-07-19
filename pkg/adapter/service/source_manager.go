package adapter

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type SourceConfig struct {
	Sources map[string]SourceInfo `json:"sources"`
}

type SourceInfo struct {
	Disabled    bool                   `json:"disabled"`
	InitialLoad bool                   `json:"initialLoad"`
	Uri         string                 `json:"uri"`
	CAFile      string                 `json:"ca_file"`
	Username    string                 `json:"username"`
	Password    string                 `json:"password"`
	AuthSource  string                 `json:"authSource"`
	DBName      string                 `json:"dbname"`
	Tables      map[string]SourceTable `json:"tables"`
}

type SourceTable struct {
	Events SourceTableEvents `json:"events"`
}

type SourceTableEvents struct {
	Create string `json:"create"`
	Update string `json:"update"`
	Delete string `json:"delete"`
}

type SourceManager struct {
	adapter *Adapter
	sources map[string]*Source
}

func NewSourceManager(adapter *Adapter) *SourceManager {
	return &SourceManager{
		adapter: adapter,
		sources: make(map[string]*Source),
	}
}

func (sm *SourceManager) Initialize() error {

	// Loading configuration file
	config, err := sm.LoadSourceConfig(viper.GetString("source.config"))
	if err != nil {
		return err
	}

	// Initializing sources
	for name, info := range config.Sources {

		if info.Disabled {
			continue
		}

		log.WithFields(log.Fields{
			"name": name,
			//"host": info.Host,
			//"port": info.Port,
			//"mode": info.Mode,
		}).Info("Initializing source")

		pwdFromEnvKey := fmt.Sprintf("%s_PASSWORD", strings.ToUpper(name))
		pwdFromEnvValue := os.Getenv(pwdFromEnvKey)
		if pwdFromEnvValue != "" {
			pwd, err := AesDecrypt(pwdFromEnvValue)
			if err != nil {
				log.Error(err)
				return err
			}

			info.Password = pwd
		}

		source := NewSource(sm.adapter, name, &info)
		err := source.Init()
		if err != nil {
			log.Error(err)
			return err
		}

		sm.sources[name] = source
	}

	return nil
}

func (sm *SourceManager) Uninit() error {
	// Loading configuration file
	config, err := sm.LoadSourceConfig(viper.GetString("source.config"))
	if err != nil {
		return err
	}

	// Initializing sources
	for name, _ := range config.Sources {
		if source, ok := sm.sources[name]; ok {
			source.Uninit()
		}
	}
	return nil
}

func (sm *SourceManager) LoadSourceConfig(filename string) (*SourceConfig, error) {

	// Open configuration file
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	// Read
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var config SourceConfig

	json.Unmarshal(byteValue, &config)

	return &config, nil
}
