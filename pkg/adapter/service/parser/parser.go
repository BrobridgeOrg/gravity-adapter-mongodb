package parser

type Parser struct {
	sqlStr      string
	sqlStrState string
	Operation   string
	Table       string
	BeforeData  map[string]*Value
	AfterData   map[string]*Value
}
