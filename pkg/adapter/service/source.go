package adapter

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BrobridgeOrg/broton"
	"github.com/spf13/viper"

	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
	parallel_chunked_flow "github.com/cfsghost/parallel-chunked-flow"
	log "github.com/sirupsen/logrus"
)

var counter uint64

type Packet struct {
	EventName string
	Payload   []byte
}

type Source struct {
	adapter   *Adapter
	info      *SourceInfo
	store     *broton.Store
	database  *Database
	connector *gravity_adapter.AdapterConnector
	incoming  chan *CDCEvent
	name      string
	parser    *parallel_chunked_flow.ParallelChunkedFlow
	tables    map[string]SourceTable
	stopping  bool
}

type Request struct {
	ResumeToken string
	Req         *Packet
	Table       string
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Request{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	// required channel
	if len(sourceInfo.Uri) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required uri")

		return nil
	}

	if len(sourceInfo.DBName) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required dbname")

		return nil
	}

	// Prepare table configs
	tables := make(map[string]SourceTable, len(sourceInfo.Tables))
	for tableName, config := range sourceInfo.Tables {
		tables[tableName] = config
	}

	source := &Source{
		adapter:  adapter,
		info:     sourceInfo,
		store:    nil,
		database: NewDatabase(),
		incoming: make(chan *CDCEvent, 204800),
		name:     name,
		tables:   tables,
	}

	// Initialize parapllel chunked flow
	pcfOpts := parallel_chunked_flow.Options{
		BufferSize: 204800,
		ChunkSize:  512,
		ChunkCount: 512,
		Handler: func(data interface{}, output func(interface{})) {
			/*
				id := atomic.AddUint64((*uint64)(&counter), 1)
				if id%100 == 0 {
					log.Info(id)
				}
			*/

			req := source.prepareRequest(data.(*CDCEvent))
			if req == nil {
				log.Error("req in nil")
				return
			}

			output(req)
		},
	}

	source.parser = parallel_chunked_flow.NewParallelChunkedFlow(&pcfOpts)

	return source
}

func (source *Source) parseEventName(event *CDCEvent) string {

	eventName := ""

	// determine event name
	tableInfo, ok := source.tables[event.Table]
	if !ok {
		log.Error("determine event name")
		return eventName
	}

	switch event.Operation {
	case InsertOperation:
		eventName = tableInfo.Events.Create
	case UpdateOperation:
		eventName = tableInfo.Events.Update
	case DeleteOperation:
		eventName = tableInfo.Events.Delete
	default:
		return eventName
	}

	return eventName
}

func (source *Source) Uninit() error {
	fmt.Println("Stopping ...")
	source.stopping = true
	source.database.stopping = true
	time.Sleep(1 * time.Second)
	<-source.connector.PublishAsyncComplete()
	source.adapter.storeMgr.Close()
	return nil

}

func (source *Source) Init() error {

	if viper.GetBool("store.enabled") {

		// Initializing store
		log.WithFields(log.Fields{
			"store": "adapter-" + source.name,
		}).Info("Initializing store for adapter")
		store, err := source.adapter.storeMgr.GetStore("adapter-" + source.name)
		if err != nil {
			return err
		}

		source.store = store

		// Register Columns
		columns := []string{"status"}
		err = source.store.RegisterColumns(columns)
		if err != nil {
			log.Error(err)
			return err
		}

		// Getting resumeToken
		var resumeToken string = ""
		resumeToken, err = source.store.GetString("status", []byte("RESUME_TOKEN"))
		if err != nil {
			log.Error(err)
			return err
		}

		source.database.resumeToken = resumeToken

	}

	// Initializing gravity adapter connector
	source.connector = source.adapter.app.GetAdapterConnector()

	// Connect to database
	err := source.database.Connect(source.info)
	if err != nil {
		return err
	}

	go source.eventReceiver()
	go source.requestHandler()

	// Getting tables
	tables := make([]string, 0, len(source.tables))
	for tableName, _ := range source.tables {
		tables = append(tables, tableName)
	}

	log.WithFields(log.Fields{
		"tables": tables,
	}).Info("Preparing to watch tables")

	log.Info("Ready to start CDC, tables: ", tables)
	// err = source.database.StartCDC(tables, source.info.InitialLoad, func(event *CDCEvent) {
	go func(tables map[string]SourceTable, initialLoad bool) {
		err = source.database.StartCDC(tables, source.info.InitialLoad, func(event *CDCEvent) {

			eventName := source.parseEventName(event)

			// filter
			if eventName == "" {
				log.Error("eventName is empty")
				return
			}

			source.incoming <- event
		})
		if err != nil {
			log.Fatal(err)
		}
	}(source.tables, source.info.InitialLoad)

	return nil
}

func (source *Source) eventReceiver() {

	log.WithFields(log.Fields{
		"source":      source.name,
		"client_name": source.adapter.clientName + "-" + source.name,
	}).Info("Initializing workers ...")

	for {
		select {
		case msg := <-source.incoming:
			source.parser.Push(msg)
		}
	}
}

func (source *Source) requestHandler() {

	for {
		select {
		case req := <-source.parser.Output():
			// TODO: retry
			if req == nil {
				log.Error("req in nil")
				break
			}
			source.HandleRequest(req.(*Request))
			requestPool.Put(req)
		}
	}
}

func (source *Source) prepareRequest(event *CDCEvent) *Request {

	// determine event name
	eventName := source.parseEventName(event)
	if eventName == "" {
		return nil
	}

	// Prepare payload
	data := make(map[string]interface{}, len(event.Before)+len(event.After))
	for k, v := range event.Before {

		data[k] = v.Data
	}

	for k, v := range event.After {

		data[k] = v.Data
	}

	payload, err := json.Marshal(data)
	if err != nil {
		log.Error(err)
		return nil
	}

	// Preparing request
	request := requestPool.Get().(*Request)
	request.ResumeToken = event.ResumeToken
	request.Table = event.Table

	request.Req = &Packet{
		EventName: eventName,
		Payload:   payload,
	}

	return request
}

func (source *Source) HandleRequest(request *Request) {

	if source.stopping {
		time.Sleep(time.Second)
		return
	}

	for {
		// Using new SDK to re-implement this part
		meta := make(map[string]string)
		meta["Nats-Msg-Id"] = fmt.Sprintf("%s-%s-%s", source.name, request.Table, request.ResumeToken)
		_, err := source.connector.PublishAsync(request.Req.EventName, request.Req.Payload, meta)
		if err != nil {
			log.Error("Failed to get publish Request:", err)
			log.Debug("EventName: ", request.Req.EventName, " Payload: ", string(request.Req.Payload))
			time.Sleep(time.Second)
			continue
		}
		log.Debug("EventName: ", request.Req.EventName)
		log.Trace("Payload: ", string(request.Req.Payload))
		break
	}

	if source.store == nil {
		id := atomic.AddUint64((*uint64)(&counter), 1)
		if id%10000 == 0 {
			<-source.connector.PublishAsyncComplete()
			counter = 0
		}
		return
	}

	for {
		err := source.store.PutString("status", []byte("RESUME_TOKEN"), request.ResumeToken)
		if err != nil {
			log.Error("Failed to update Position Name")
			time.Sleep(time.Second)
			continue
		}
		break
	}

	id := atomic.AddUint64((*uint64)(&counter), 1)
	if id%10000 == 0 {
		<-source.connector.PublishAsyncComplete()
		counter = 0
	}
}
