package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	clientOptions := options.Client().ApplyURI(info.Uri)

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

func (database *Database) StartCDC(tables map[string]SourceTable, initialLoad bool, fn func(*CDCEvent)) error {

	db := database.GetConnection()

	var wg sync.WaitGroup

	for tableName := range tables {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()

			for {
				log.WithFields(log.Fields{
					"Table": tableName,
				}).Info("Start Watch Event.")

				matchStage := bson.D{}
				opts := options.ChangeStream().SetMaxAwaitTime(2 * time.Second)
				if initialLoad && database.resumeToken == "" {
					// InitialLoad
					opts.SetStartAtOperationTime(&primitive.Timestamp{
						T: 0,
					})
				} else if database.resumeToken != "" {
					rt := bson.Raw{}
					bson.Unmarshal([]byte(database.resumeToken), &rt)
					opts.SetResumeAfter(rt)
				}

				changeStream, err := db.Collection(tableName).Watch(context.TODO(), matchStage, opts)
				if err != nil {
					log.WithFields(log.Fields{
						"Table": tableName,
					}).Error(err)
					return
				}

				defer changeStream.Close(context.TODO())

				for changeStream.Next(context.TODO()) {
					event := make(map[string]interface{}, 0)
					changeStream.Decode(event)
					resumeToken := changeStream.ResumeToken()
					database.resumeToken = string(resumeToken)

					// parsing event
					cdcEvent, err := database.processEvent(event)
					if err != nil {
						log.WithFields(log.Fields{
							"Table": tableName,
						}).Error(err)
						continue
					}

					// add resumeToken to cdcEvent
					cdcEvent.ResumeToken = database.resumeToken
					//log.Info("resumeToken: ", database.resumeToken)

					// Send event
					fn(cdcEvent)
				}
			}
		}(tableName)
	}

	wg.Wait()

	return nil
}
