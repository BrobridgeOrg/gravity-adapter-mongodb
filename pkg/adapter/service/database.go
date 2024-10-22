package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabaseInfo struct {
	Uri    string `json:"uri"`
	CAFile int    `json:"ca_file"`
	DbName string `json:"db_name"`
}
type Database struct {
	dbInfo      *DatabaseInfo
	db          *mongo.Database
	resumeToken string
	stopping    bool
}

func NewDatabase() *Database {
	return &Database{
		dbInfo: &DatabaseInfo{},
	}
}

func (database *Database) LoadCert(caFile string) (*tls.Config, error) {

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil

}

func (database *Database) Connect(info *SourceInfo) error {

	log.WithFields(log.Fields{
		"uri":    info.Uri,
		"dbname": info.DBName,
	}).Info("Connecting to database")

	targetTables := make([]string, 0, len(info.Tables))
	for tableName, _ := range info.Tables {
		targetTables = append(targetTables, tableName)
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(info.Uri).
		SetMaxPoolSize(10).
		SetMinPoolSize(10).
		SetMaxConnIdleTime(10 * time.Minute).
		SetHeartbeatInterval(10 * time.Second).
		SetSocketTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	// Set auth
	if len(info.Username) > 0 && len(info.Password) > 0 {
		clientOptions.SetAuth(options.Credential{
			Username:   info.Username,
			Password:   info.Password,
			AuthSource: info.AuthSource,
		})
	}

	// Load CA file
	if len(info.CAFile) > 0 {
		tlsConfig, err := database.LoadCert(info.CAFile)
		if err != nil {
			//log.Error(err)
			return err
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}

	database.db = client.Database(info.DBName)

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	log.Info("Connected to MongoDB Successfully")

	return nil
}

func (database *Database) GetConnection() *mongo.Database {
	return database.db
}

func (database *Database) doInitialLoad(tables map[string]SourceTable, fn func(*CDCEvent)) error {
	db := database.GetConnection()
	for tableName := range tables {
		counter := 0
		opts := options.Find().SetBatchSize(100)
		ctx := context.Background()
		cursor, err := db.Collection(tableName).Find(ctx, bson.M{}, opts)
		if err != nil {
			log.Error(err)
			continue
		}

		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				log.Printf("Error decoding document: %v", err)
				continue
			}
			cdcEvent, err := database.parseSnapshotEvent(tableName, doc) // 處理現有文檔
			if err != nil {
				log.Error(err)
				continue
			}
			//log.Info(cdcEvent)
			counter++
			cdcEvent.ResumeToken = "snapshot-" + tableName + "-" + strconv.Itoa(counter)
			fn(cdcEvent)
		}

		if err := cursor.Err(); err != nil {
			log.Error(err)
			continue
		}
		cursor.Close(ctx)
	}
	return nil

}
func (database *Database) StartCDC(tables map[string]SourceTable, initialLoad bool, fn func(*CDCEvent)) error {

	if initialLoad && database.resumeToken == "" {
		err := database.doInitialLoad(tables, fn)
		if err != nil {
			return err
		}
		initialLoad = false
		database.resumeToken = ""
	}
	if strings.HasPrefix(database.resumeToken, "snapshot-") {
		database.resumeToken = ""
	}

	db := database.GetConnection()

	var wg sync.WaitGroup

	for tableName := range tables {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			var csid interface{}

			for {
				log.WithFields(log.Fields{
					"Table": tableName,
				}).Info("Start Watch Event.")

				matchStage := bson.D{}
				opts := options.ChangeStream().SetMaxAwaitTime(2 * time.Second)

				if csid != nil && database.resumeToken == "" {
					//invalidate resumeToken
					opts.SetStartAfter(csid)
				} else if database.resumeToken != "" {
					rt := bson.Raw{}
					bson.Unmarshal([]byte(database.resumeToken), &rt)
					opts.SetResumeAfter(rt)
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				changeStream, err := db.Collection(tableName).Watch(ctx, matchStage, opts)
				if err != nil {
					log.WithFields(log.Fields{
						"Table": tableName,
					}).Error(err)
					log.Info("Retry ...")
					cancel()
					time.Sleep(1 * time.Second)
					continue
				}

				defer changeStream.Close(ctx)

				for {
					// Check stopping flag periodically
					if database.stopping {
						log.WithFields(log.Fields{
							"Table": tableName,
						}).Info("Shutdown flag is set. Exiting ChangeStream loop.")
						cancel()
						return
					}
					if changeStream.Next(ctx) {

						event := make(map[string]interface{}, 0)
						if err := changeStream.Decode(&event); err != nil {
							log.WithFields(log.Fields{
								"Table": tableName,
							}).Error("Error decoding event:", err, ", Skip ...")
							continue
						}

						resumeToken := changeStream.ResumeToken()
						database.resumeToken = string(resumeToken)

						// parsing event
						cdcEvent, err := database.processEvent(event)
						if err != nil {
							if event["operationType"].(string) == "invalidate" {
								log.Warn("Change stream has been invalidated. The application will attempt to reset the change stream token and resume monitoring.")
								if id, ok := event["_id"].(interface{}); ok {
									csid = id
								}

								database.resumeToken = ""
								break
							}
							log.WithFields(log.Fields{
								"Table": tableName,
							}).Error(err, ", Skip ...")
							continue
						}

						// add resumeToken to cdcEvent
						cdcEvent.ResumeToken = database.resumeToken

						// Send event
						// TODO Replace event workaround send delete and insert
						if cdcEvent.Operation == ReplaceOperation {
							cdcEventDelete := cdcEventPool.Get().(*CDCEvent)
							cdcEventDelete.ReplaceOp = database.resumeToken
							cdcEventDelete.Operation = DeleteOperation
							cdcEventDelete.Table = cdcEvent.Table
							cdcEventDelete.After = cdcEvent.After
							cdcEventDelete.Before = cdcEvent.Before
							fn(cdcEventDelete)

							cdcEventInsert := cdcEventPool.Get().(*CDCEvent)
							cdcEventInsert.ReplaceOp = database.resumeToken
							cdcEventInsert.Operation = InsertOperation
							cdcEventInsert.Table = cdcEvent.Table
							cdcEventInsert.After = cdcEvent.After
							fn(cdcEventInsert)

							cdcEventPool.Put(cdcEvent)
						} else {
							fn(cdcEvent)
						}
					} else if err := changeStream.Err(); err != nil {
						log.WithFields(log.Fields{
							"Table": tableName,
						}).Error("ChangeStream encountered an error:", err)
						cancel()
						log.Info("Retry ...")
						break
					}
				}

			}
		}(tableName)
	}

	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database.db.Client().Disconnect(ctx)
	log.Info("Disconnect DB")

	return nil
}
