package constants

/*
overflow_checks.go

This package provides overflow checking utilities for index calculations.

MED-011: Comprehensive overflow protection for index calculations:
- Bucket calculations
- Page number calculations
- Sequence number increments
- TotalRecords increments
- BucketCount increments

Design Principles:
- Validate ranges before calculations
- Use uint64 where appropriate
- Provide clear error messages
- Prevent crashes and corruption
*/

import (
	"fmt"
)

// MaxUint32 is the maximum value for uint32 (4,294,967,295)
const MaxUint32 = ^uint32(0)

// MaxUint64 is the maximum value for uint64 (18,446,744,073,709,551,615)
const MaxUint64 = ^uint64(0)

// MaxUint32Safe is a safe maximum that leaves room for increment operations
// This prevents overflow in calculations like value + 1
const MaxUint32Safe = MaxUint32 - 1000

// MaxUint64Safe is a safe maximum that leaves room for increment operations
// This prevents overflow in calculations like value + 1
const MaxUint64Safe = MaxUint64 - 1000000

// OverflowError represents an integer overflow condition
type OverflowError struct {
	Operation  string
	Value      interface{}
	MaxValue   interface{}
	FieldName  string
}

func (e *OverflowError) Error() string {
	return fmt.Sprintf("integer overflow in %s: %v exceeds maximum value %v for field %s",
		e.Operation, e.Value, e.MaxValue, e.FieldName)
}

// CheckUint32Increment checks if incrementing a uint32 value would cause overflow
// Returns an error if increment would overflow, nil otherwise
func CheckUint32Increment(value uint32, fieldName string) error {
	if value >= MaxUint32Safe {
		return &OverflowError{
			Operation: "increment",
			Value:     value,
			MaxValue:  MaxUint32Safe,
			FieldName: fieldName,
		}
	}
	return nil
}

// CheckUint64Increment checks if incrementing a uint64 value would cause overflow
// Returns an error if increment would overflow, nil otherwise
func CheckUint64Increment(value uint64, fieldName string) error {
	if value >= MaxUint64Safe {
		return &OverflowError{
			Operation: "increment",
			Value:     value,
			MaxValue:  MaxUint64Safe,
			FieldName: fieldName,
		}
	}
	return nil
}

// SafeUint32Increment safely increments a uint32 value with overflow check
// Returns the incremented value and an error if overflow would occur
func SafeUint32Increment(value uint32, fieldName string) (uint32, error) {
	if err := CheckUint32Increment(value, fieldName); err != nil {
		return value, err
	}
	return value + 1, nil
}

// SafeUint64Increment safely increments a uint64 value with overflow check
// Returns the incremented value and an error if overflow would occur
func SafeUint64Increment(value uint64, fieldName string) (uint64, error) {
	if err := CheckUint64Increment(value, fieldName); err != nil {
		return value, err
	}
	return value + 1, nil
}

// CheckUint32Addition checks if adding two uint32 values would cause overflow
// Returns an error if addition would overflow, nil otherwise
func CheckUint32Addition(a, b uint32, fieldName string) error {
	if a > MaxUint32-b {
		return &OverflowError{
			Operation: "addition",
			Value:     fmt.Sprintf("%d + %d", a, b),
			MaxValue:  MaxUint32,
			FieldName: fieldName,
		}
	}
	return nil
}

// CheckUint64Addition checks if adding two uint64 values would cause overflow
// Returns an error if addition would overflow, nil otherwise
func CheckUint64Addition(a, b uint64, fieldName string) error {
	if a > MaxUint64-b {
		return &OverflowError{
			Operation: "addition",
			Value:     fmt.Sprintf("%d + %d", a, b),
			MaxValue:  MaxUint64,
			FieldName: fieldName,
		}
	}
	return nil
}

// CheckUint32Multiplication checks if multiplying two uint32 values would cause overflow
// Returns an error if multiplication would overflow, nil otherwise
func CheckUint32Multiplication(a, b uint32, fieldName string) error {
	if a != 0 && b > MaxUint32/a {
		return &OverflowError{
			Operation: "multiplication",
			Value:     fmt.Sprintf("%d * %d", a, b),
			MaxValue:  MaxUint32,
			FieldName: fieldName,
		}
	}
	return nil
}

// CheckUint64Multiplication checks if multiplying two uint64 values would cause overflow
// Returns an error if multiplication would overflow, nil otherwise
func CheckUint64Multiplication(a, b uint64, fieldName string) error {
	if a != 0 && b > MaxUint64/a {
		return &OverflowError{
			Operation: "multiplication",
			Value:     fmt.Sprintf("%d * %d", a, b),
			MaxValue:  MaxUint64,
			FieldName: fieldName,
		}
	}
	return nil
}

// CheckBucketCount validates that a bucket count is within safe limits
// Returns an error if bucket count is invalid, nil otherwise
func CheckBucketCount(bucketCount uint32) error {
	if bucketCount == 0 {
		return fmt.Errorf("bucket count cannot be zero")
	}
	// Maximum practical bucket count (2^20 = 1,048,576 buckets)
	// Beyond this, performance degrades significantly
	const MaxPracticalBucketCount = 1 << 20
	if bucketCount > MaxPracticalBucketCount {
		return fmt.Errorf("bucket count %d exceeds maximum practical limit %d", bucketCount, MaxPracticalBucketCount)
	}
	return nil
}

// CheckPageNumber validates that a page number is within safe limits
// Returns an error if page number is invalid, nil otherwise
func CheckPageNumber(pageNum uint32) error {
	if pageNum >= MaxUint32Safe {
		return &OverflowError{
			Operation: "page number validation",
			Value:     pageNum,
			MaxValue:  MaxUint32Safe,
			FieldName: "PageNumber",
		}
	}
	return nil
}

// CheckSequenceNumber validates that a sequence number is within safe limits
// Returns an error if sequence number is invalid, nil otherwise
func CheckSequenceNumber(sequence uint64) error {
	if sequence >= MaxUint64Safe {
		return &OverflowError{
			Operation: "sequence number validation",
			Value:     sequence,
			MaxValue:  MaxUint64Safe,
			FieldName: "Sequence",
		}
	}
	return nil
}

// SafeBucketCalculation safely calculates a bucket number with overflow protection
// Returns the bucket number and an error if calculation would overflow
func SafeBucketCalculation(hashValue uint32, numBuckets uint32) (uint32, error) {
	if err := CheckBucketCount(numBuckets); err != nil {
		return 0, err
	}
	// Modulo operation is safe - it always returns a value in [0, numBuckets)
	return hashValue % numBuckets, nil
}
