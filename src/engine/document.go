package engine

type Document struct {
	DocumentID  string
	Name        string
	Description string
	Fields      map[string]Field
}
