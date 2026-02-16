// Cross-validate: feed JSONL IR through TS compiler and compare with Go-compiled syndrql
const { compile } = require('../dist/src/compiler');
const fs = require('fs');
const path = require('path');

const dataPath = path.join(__dirname, '../../training_data.jsonl');
const lines = fs.readFileSync(dataPath, 'utf-8').trim().split('\n');

let pass = 0;
let fail = 0;

for (let i = 0; i < lines.length; i++) {
  const example = JSON.parse(lines[i]);
  const goOutput = example.syndrql;

  try {
    const stmt = example.ir.statements[0];
    const tsOutput = compile(stmt);

    if (tsOutput === goOutput) {
      pass++;
    } else {
      fail++;
      if (fail <= 10) { // Only show first 10 mismatches
        console.log(`MISMATCH line ${i + 1} (${stmt.statementType}):`);
        console.log(`  Go: ${goOutput}`);
        console.log(`  TS: ${tsOutput}`);
      }
    }
  } catch (err) {
    fail++;
    if (fail <= 10) {
      console.log(`ERROR line ${i + 1}: ${err.message}`);
    }
  }
}

console.log(`\nResults: ${pass} pass, ${fail} fail out of ${lines.length} total`);
if (fail > 0) {
  process.exit(1);
} else {
  console.log('Cross-validation PASSED: Go and TS compilers produce identical output!');
}
