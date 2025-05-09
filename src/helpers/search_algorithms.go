package helpers

import "sort"

func FindByKeyValueBinarySearch(pairs []KeyValue, targetKey, targetValue string) (KeyValue, bool) {
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value < pairs[j].Value
	})

	low, high := 0, len(pairs)-1
	for low <= high {
		mid := (low + high) / 2
		if pairs[mid].Value == targetValue && pairs[mid].Key == targetKey {
			return pairs[mid], true
		} else if pairs[mid].Value < targetValue {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return KeyValue{}, false
}
