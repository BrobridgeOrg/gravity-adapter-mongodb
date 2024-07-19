package app

import (
	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
)

type App interface {
	GetAdapterConnector() *gravity_adapter.AdapterConnector
}
