package adapter

import (
	"fmt"
	"os"
	"strings"

	"git.brobridge.com/gravity/gravity-adapter-mongodb/pkg/app"
	"github.com/BrobridgeOrg/broton"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Adapter struct {
	app        app.App
	storeMgr   *broton.Broton
	sm         *SourceManager
	clientName string
}

func NewAdapter(a app.App) *Adapter {
	adapter := &Adapter{
		app: a,
	}

	adapter.sm = NewSourceManager(adapter)

	return adapter
}

func (adapter *Adapter) Init() error {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return err
	}

	host = strings.ReplaceAll(host, ".", "_")

	adapter.clientName = fmt.Sprintf("gravity_adapter_mongodb-%s", host)

	// Initializing store manager
	viper.SetDefault("store.enabled", false)
	enabled := viper.GetBool("store.enabled")
	if enabled {
		viper.SetDefault("store.path", "./store")
		options := broton.NewOptions()
		options.DatabasePath = viper.GetString("store.path")

		log.WithFields(log.Fields{
			"path": options.DatabasePath,
		}).Info("Initializing store")
		broton, err := broton.NewBroton(options)
		if err != nil {
			return err
		}

		adapter.storeMgr = broton
	}

	err = adapter.sm.Initialize()
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (adapter *Adapter) Uninit() error {
	return adapter.sm.Uninit()
}
