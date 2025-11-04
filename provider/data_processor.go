package provider

type Kv struct {
	Key   string
	Value any
}

type dataProcessor struct {
}

func newDataProcessor() *dataProcessor {
	return &dataProcessor{}
}
