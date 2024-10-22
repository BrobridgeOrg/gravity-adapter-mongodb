package main

import (
	"fmt"
	_ "go.uber.org/automaxprocs"
	"os"
	//	"os/signal"
	//	"time"
	//	"runtime"
	//	"runtime/pprof"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	app "git.brobridge.com/gravity/gravity-adapter-mongodb/pkg/app/instance"
)

func init() {

	debugLevel := log.InfoLevel
	switch os.Getenv("GRAVITY_DEBUG") {
	case log.TraceLevel.String():
		debugLevel = log.TraceLevel
	case log.DebugLevel.String():
		debugLevel = log.DebugLevel
	case log.ErrorLevel.String():
		debugLevel = log.ErrorLevel
	}

	log.SetLevel(debugLevel)

	fmt.Printf("Debug level is set to \"%s\"\n", debugLevel.String())

	// From the environment
	viper.SetEnvPrefix("GRAVITY_ADAPTER_MONGODB")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// From config file
	viper.SetConfigName("config")
	viper.AddConfigPath("./")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		log.Warn("No configuration file was loaded")
	}

	//runtime.GOMAXPROCS(8)
	/*
		go func() {

			f, err := os.Create("mem-profile.prof")
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			//runtime.GC()
			time.Sleep(30 * time.Second)
			pprof.WriteHeapProfile(f)
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, os.Kill)
			<-sig
		}()

		go func() {

			f, err := os.Create("cpu-profile.prof")
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()

			sig := make(chan os.Signal, 1)
			signal.Notify(sig, os.Interrupt, os.Kill)
			<-sig
			os.Exit(0)
		}()
	*/
}

func main() {

	// Initializing application
	a := app.NewAppInstance()

	err := a.Init()
	if err != nil {
		log.Fatal(err)
		return
	}

	// Starting application
	err = a.Run()
	if err != nil {
		log.Fatal(err)
		return
	}
}
