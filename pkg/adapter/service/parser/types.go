package parser

type Value struct {
	Type   ValueType
	Params []*Value
	Data   interface{}
}

type ValueType uint8

const (
	UnknownType = ValueType(iota + 1)
	FuncValueType
)

func NewValue() *Value {
	return &Value{
		Params: make([]*Value, 0),
	}
}
