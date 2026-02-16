package main

import (
	"flag"
	"fmt"
	"os"

	"syndrql-training/codegen"
	"syndrql-training/generator"
	"syndrql-training/ir"
)

func main() {
	count := flag.Int("count", 500, "Number of training examples to generate")
	seed := flag.Int64("seed", 42, "Random seed for reproducible generation")
	output := flag.String("output", "training_data.jsonl", "Output JSONL file path")
	schemaPath := flag.String("schema", "", "Export JSON Schema to this path")
	codegenTS := flag.String("codegen-ts", "", "Generate TypeScript interfaces to this directory")

	flag.Parse()

	// JSON Schema export mode
	if *schemaPath != "" {
		if err := ir.WriteJSONSchema(*schemaPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing schema: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("JSON Schema written to %s\n", *schemaPath)
		if *codegenTS == "" && *output == "training_data.jsonl" {
			return
		}
	}

	// TypeScript codegen mode
	if *codegenTS != "" {
		if err := codegen.GenerateTypeScript(*codegenTS); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating TypeScript: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("TypeScript types generated in %s\n", *codegenTS)
		if *output == "training_data.jsonl" && *schemaPath == "" {
			return
		}
	}

	// Training data generation mode
	gen := generator.New(*seed, *count)
	if err := gen.Generate(*output); err != nil {
		fmt.Fprintf(os.Stderr, "Error generating training data: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Generated %d training examples to %s (seed=%d)\n", *count, *output, *seed)
}
