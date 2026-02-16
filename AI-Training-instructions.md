# SyndrQL AI Training Data Generator

## Overview

This toolchain generates training data (JSONL) for fine-tuning language models to convert natural language into SyndrQL via a structured IR (intermediate representation). Each training example is a `(NL, IR, SyndrQL)` triple where the IR compiles deterministically to exactly one SyndrQL string.

The IR→SyndrQL compiler exists in both Go (used during generation for validation) and TypeScript (runtime compiler for applications consuming model output).

## CLI Usage

### Build

```bash
cd scripts/syndrql-training && go build -o ../../bin/training-gen .
```

### Generate Training Data

```bash
# Generate 500 examples with seed 42
../../bin/training-gen -count 500 -seed 42 -output training_data.jsonl

# Generate a larger dataset
../../bin/training-gen -count 5000 -seed 100 -output training_data_large.jsonl

# Different seeds produce different permutations
../../bin/training-gen -count 500 -seed 1 -output batch_a.jsonl
../../bin/training-gen -count 500 -seed 2 -output batch_b.jsonl
```

### Export JSON Schema

```bash
../../bin/training-gen -schema ir_schema.json
```

### Regenerate TypeScript Types from Go Structs

```bash
../../bin/training-gen -codegen-ts ../../syndrql-ir-compiler/src/generated/
```

### Run Go Compiler Tests

```bash
cd scripts/syndrql-training && go test ./...
```

### TypeScript Compiler

```bash
cd syndrql-ir-compiler
npm install
npm run build
npm test
```

### Validate Generated Output

```bash
# Verify JSON is well-formed
cat training_data.jsonl | python3 -c "import sys,json; [json.loads(l) for l in sys.stdin]; print('OK')"

# Spot-check first 5 examples
head -5 training_data.jsonl | python3 -c "import sys,json; [print(json.dumps(json.loads(l),indent=2)) for l in sys.stdin]"

# Check statement type distribution
cat training_data.jsonl | python3 -c "
import sys, json, collections
c = collections.Counter()
for l in sys.stdin:
    obj = json.loads(l)
    for s in obj['ir']['statements']:
        c[s['statementType']] += 1
for k, v in c.most_common():
    print(f'{k}: {v}')
"
```

### Cross-Validate Go and TypeScript Compilers

```bash
cd syndrql-ir-compiler && node tests/cross-validate.js
```

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-count` | `500` | Number of training examples to generate |
| `-seed` | `42` | Random seed for reproducible generation |
| `-output` | `training_data.jsonl` | Output JSONL file path |
| `-schema` | | Export JSON Schema to this path (then exit) |
| `-codegen-ts` | | Generate TypeScript interfaces to this directory (then exit) |

## Output Format

Each line of the JSONL output is a JSON object with three fields:

```json
{
  "nl": "Show me all customers ordered by name",
  "ir": {
    "statements": [{ "statementType": "select", ... }],
    "explanation": "Show me all customers ordered by name",
    "confidence": 0.95
  },
  "syndrql": "SELECT * FROM \"Customers\" ORDER BY name ASC;"
}
```

## Statement Type Distribution (per 500 examples)

| Category | Count | % |
|----------|-------|---|
| SELECT (15 variants) | ~260 | 52% |
| INSERT | ~40 | 8% |
| UPDATE | ~35 | 7% |
| DELETE | ~25 | 5% |
| CREATE/DROP/UPDATE BUNDLE | ~30 | 6% |
| CREATE INDEX | ~15 | 3% |
| Views | ~15 | 3% |
| Transactions | ~15 | 3% |
| Cursors | ~10 | 2% |
| Prepared Statements | ~10 | 2% |
| RBAC | ~15 | 3% |
| SHOW/Utility | ~15 | 3% |
| Database commands | ~10 | 2% |
| Migration/Ops | ~5 | 1% |

## Project Structure

```
scripts/syndrql-training/          # Go: generator + compiler + codegen
├── main.go                        # CLI entry point
├── ir/                            # IR type definitions
│   ├── types.go                   # 60+ statement structs
│   ├── expression.go              # Expression tree types
│   └── jsonschema.go              # JSON Schema generation
├── compiler/                      # Go IR→SyndrQL compiler
│   ├── compiler.go                # Statement dispatch
│   ├── expression_compiler.go     # Expression compilation
│   └── compiler_test.go           # 40+ test cases
├── codegen/
│   └── typescript.go              # Go struct → TypeScript interface codegen
└── generator/                     # Training data generators
    ├── generator.go               # Orchestrator
    ├── vocabulary.go              # Bundle schemas, NL templates, sample values
    ├── select_gen.go              # 15 SELECT sub-generators
    ├── dml_gen.go                 # INSERT/UPDATE/DELETE
    ├── ddl_gen.go                 # CREATE/DROP BUNDLE, CREATE INDEX
    ├── view_gen.go                # Views
    ├── transaction_gen.go         # Transactions
    ├── cursor_gen.go              # Cursors
    ├── prepared_gen.go            # Prepared statements
    ├── rbac_gen.go                # Users/Roles/Grants
    ├── utility_gen.go             # SHOW/EXPLAIN
    ├── database_gen.go            # Database management
    └── migration_gen.go           # Migrations/Checkpoint

syndrql-ir-compiler/               # TypeScript: npm-publishable IR compiler
├── package.json                   # @syndrdb/ir-compiler
├── tsconfig.json
├── src/
│   ├── generated/
│   │   └── types.ts               # Auto-generated from Go structs
│   ├── compiler.ts                # TS IR→SyndrQL compiler
│   ├── expression-compiler.ts     # Expression compilation
│   └── index.ts                   # Public API
└── tests/
    ├── compiler.test.ts           # 56 test cases (mirrors Go tests)
    └── cross-validate.js          # Cross-validates Go and TS output
```
