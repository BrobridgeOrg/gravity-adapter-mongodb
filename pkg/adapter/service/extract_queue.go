package adapter

import (
	"errors"
	"sync"

	parser "git.brobridge.com/gravity/gravity-adapter-mongodb/pkg/adapter/service/parser"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OperationType int8

const (
	InsertOperation = OperationType(iota + 1)
	UpdateOperation
	DeleteOperation
	ReplaceOperation
)

type CDCEvent struct {
	ResumeToken            string
	Operation              OperationType
	Table                  string
	After                  map[string]*parser.Value
	Before                 map[string]*parser.Value
	ReplaceOp              string
	UpdateEventRemoveField map[string]*parser.Value
}

var cdcEventPool = sync.Pool{
	New: func() interface{} {
		return &CDCEvent{}
	},
}

func (database *Database) parseSnapshotEvent(table string, event map[string]interface{}) (*CDCEvent, error) {
	afterValue := make(map[string]*parser.Value)
	for key, value := range event {
		afterValue[key] = &parser.Value{
			Data: value,
		}
	}

	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = InsertOperation
	result.Table = table
	result.After = afterValue

	return result, nil
}

func (database *Database) parseInsertSQL(event map[string]interface{}) (*CDCEvent, error) {

	// Parsing event
	if event["fullDocument"] == nil {
		return nil, errors.New("Insert event parsing error(fullDocument not found).")
	}
	e := event["fullDocument"].(map[string]interface{})

	afterValue := make(map[string]*parser.Value)
	for key, value := range e {
		afterValue[key] = &parser.Value{
			Data: value,
		}
	}

	if event["ns"] == nil {
		return nil, errors.New("Insert event parsing error(table not found).")
	}
	ns := event["ns"].(map[string]interface{})
	table := ns["coll"].(string)

	// Prepare CDC event
	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = InsertOperation
	result.Table = table
	result.After = afterValue

	return result, nil
}

func (database *Database) parseReplaceSQL(event map[string]interface{}) (*CDCEvent, error) {

	// Parsing event
	if event["documentKey"] == nil {
		return nil, errors.New("Replace event parsing error(documentKey not found).")
	}
	dk := event["documentKey"].(map[string]interface{})

	beforeValue := make(map[string]*parser.Value)
	for key, value := range dk {
		beforeValue[key] = &parser.Value{
			Data: value,
		}
	}

	// Parsing event
	if event["fullDocument"] == nil {
		return nil, errors.New("Replace event parsing error(fullDocument not found).")
	}
	e := event["fullDocument"].(map[string]interface{})

	afterValue := make(map[string]*parser.Value)
	for key, value := range e {
		afterValue[key] = &parser.Value{
			Data: value,
		}
	}

	if event["ns"] == nil {
		return nil, errors.New("Replace event parsing error(table not found).")
	}
	ns := event["ns"].(map[string]interface{})
	table := ns["coll"].(string)

	// Prepare CDC event
	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = ReplaceOperation
	result.Table = table
	result.After = afterValue
	result.Before = beforeValue

	return result, nil
}

func (database *Database) parseUpdateSQL(event map[string]interface{}) (*CDCEvent, error) {

	// Parsing event
	if event["documentKey"] == nil {
		return nil, errors.New("Update event parsing error(documentKey not found).")
	}
	dk := event["documentKey"].(map[string]interface{})

	beforeValue := make(map[string]*parser.Value)
	for key, value := range dk {
		beforeValue[key] = &parser.Value{
			Data: value,
		}
	}

	if event["updateDescription"] == nil {
		return nil, errors.New("Update event parsing error(updateDescription not found).")
	}
	updateDesc := event["updateDescription"].(map[string]interface{})
	updateData := updateDesc["updatedFields"].(map[string]interface{})

	afterValue := make(map[string]*parser.Value)
	for key, value := range updateData {
		afterValue[key] = &parser.Value{
			Data: value,
		}
	}

	removeData := updateDesc["removedFields"].(primitive.A)
	removeValue := make(map[string]*parser.Value)
	for _, field := range removeData {
		removeValue[field.(string)] = &parser.Value{
			Data: nil,
		}
	}

	if event["ns"] == nil {
		return nil, errors.New("Update event parsing error(table not found).")
	}
	ns := event["ns"].(map[string]interface{})
	table := ns["coll"].(string)

	// Prepare CDC event
	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = UpdateOperation
	result.Table = table
	result.After = afterValue
	result.Before = beforeValue
	result.UpdateEventRemoveField = removeValue

	return result, nil
}

func (database *Database) parseDeleteSQL(event map[string]interface{}) (*CDCEvent, error) {
	//TODO
	// Parsing event
	if event["documentKey"] == nil {
		return nil, errors.New("Delete event parsing error(documentKey not found).")
	}
	dk := event["documentKey"].(map[string]interface{})

	beforeValue := make(map[string]*parser.Value)
	for key, value := range dk {
		beforeValue[key] = &parser.Value{
			Data: value,
		}
	}

	if event["ns"] == nil {
		return nil, errors.New("Delete event parsing error(table not found).")
	}
	ns := event["ns"].(map[string]interface{})
	table := ns["coll"].(string)

	// Prepare CDC event
	result := cdcEventPool.Get().(*CDCEvent)
	result.Operation = DeleteOperation
	result.Table = table
	result.Before = beforeValue

	return result, nil
}

func (database *Database) processEvent(event map[string]interface{}) (*CDCEvent, error) {

	switch event["operationType"].(string) {
	case "insert":
		return database.parseInsertSQL(event)
	case "update":
		return database.parseUpdateSQL(event)
	case "delete":
		return database.parseDeleteSQL(event)
	case "replace":
		return database.parseReplaceSQL(event)
	}

	return nil, errors.New("Unsupported operation: " + event["operationType"].(string))
}
