package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
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

	// Load CA file
	if len(info.CAFile) > 0 {
		tlsConfig, err := database.LoadCert(info.CAFile)
		if err != nil {
			log.Error(err)
			return err
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return err
	}

	database.db = client.Database(info.DBName)

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	log.Info("Connected to MongoDB Successfully")

	return nil
}

func (database *Database) GetConnection() *mongo.Database {
	return database.db
}

func (database *Database) StartCDC(tables []string, initialLoad bool, fn func(*CDCEvent)) error {

	db := database.GetConnection()

	for {

		log.Info("Start Watch Event.")

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

		changeStream, err := db.Watch(context.TODO(), matchStage, opts)
		if err != nil {
			<-time.After(1 * time.Second)
			log.Error(err)
			continue
		}

		for changeStream.Next(context.TODO()) {
			event := make(map[string]interface{}, 0)
			changeStream.Decode(event)
			resumeToken := changeStream.ResumeToken()
			database.resumeToken = string(resumeToken)

			// parsing event
			cdcEvent, err := database.processEvent(event)
			if err != nil {
				log.Error(err)
				continue
			}

			// add resumeToken to cdcEvent
			cdcEvent.ResumeToken = database.resumeToken
			//log.Info("resumeToken: ", database.resumeToken)

			// Send event
			fn(cdcEvent)
		}
	}

	return nil
}
