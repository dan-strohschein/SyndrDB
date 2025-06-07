package btreeindex

import (
	"fmt"
)

// Example of using the tournament sorter
func ExampleUsage() {
	// Create a tournament sorter with 10MB memory limit
	sorter := NewTournamentSorter(10*1024*1024, "", nil)
	defer sorter.Cleanup() // Clean up temp files when done

	// Add some sample documents
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		docID := fmt.Sprintf("doc%d", i)
		extraData := []byte(fmt.Sprintf("extra%d", i))

		if err := sorter.Add(key, docID, extraData); err != nil {
			fmt.Printf("Error adding item: %v\n", err)
			return
		}
	}

	// Sort and get iterator
	iter, err := sorter.Sort()
	if err != nil {
		fmt.Printf("Error sorting: %v\n", err)
		return
	}
	defer iter.Close()

	// Use the iterator to build the B-tree index
	count := 0
	for {
		// kv, ok := iter.Next()
		// if !ok {
		// 	break
		// }

		// Here you would add the entry to your B-tree
		// btree.Insert(kv.Key, kv.DocID, kv.ExtraData)

		count++
		if count%10000 == 0 {
			fmt.Printf("Processed %d entries\n", count)
		}
	}

	fmt.Printf("Total processed: %d entries\n", count)
}
