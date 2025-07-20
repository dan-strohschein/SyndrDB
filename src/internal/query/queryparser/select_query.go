package queryparser

type SelectQuery struct {
	Fields []string
	Where  string
	Limit  int
	Offset int
}

// TODO bring our code that parses a query string into this struct
