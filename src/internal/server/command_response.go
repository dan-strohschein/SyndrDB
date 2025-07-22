package server

type CommandResponse struct {
	ResultCount int
	Result      interface{}
}

type QueryResponse struct {
	ResultCount int
	Results     *[]DocumentResponse
}

type DocumentResponse struct {
	Fields   map[string]interface{}   // "FieldName" : "FieldValue"
	Includes map[string][]interface{} // "BundleName" : []*models.Document
}
