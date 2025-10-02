package server

type CommandResponse struct {
	ResultCount     int
	Result          interface{}
	ExecutionTimeMS float64
}

type QueryResponse struct {
	ResultCount     int
	Results         *[]DocumentResponse
	ExecutionTimeMS float64
}

type DocumentResponse struct {
	Fields   map[string]interface{}   // "FieldName" : "FieldValue"
	Includes map[string][]interface{} // "BundleName" : []*models.Document
}
