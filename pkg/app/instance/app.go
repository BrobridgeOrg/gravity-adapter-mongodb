package instance

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	adapter_service "git.brobridge.com/gravity/gravity-adapter-mongodb/pkg/adapter/service"
	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done             chan os.Signal
	adapter          *adapter_service.Adapter
	adapterConnector *gravity_adapter.AdapterConnector
}

func NewAppInstance() *AppInstance {

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	a := &AppInstance{
		done: sig,
	}

	a.adapter = adapter_service.NewAdapter(a)

	return a
}

func (a *AppInstance) Init() error {

	log.WithFields(log.Fields{
		"max_procs": runtime.GOMAXPROCS(0),
	}).Info("Starting application")

	// Initializing adapter connector
	err := a.initAdapterConnector()
	if err != nil {
		return err
	}

	err = a.adapter.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
	a.adapter.Uninit()
}

func (a *AppInstance) Run() error {

	<-a.done
	a.Uninit()
	fmt.Println("Bye!")
	return nil
}
