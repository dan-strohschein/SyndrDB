package main

import "go.uber.org/zap"

func main() {
	// helpers.Init()
	// ColorLogger := helpers.SetupLogger()
	// var HighlightGreen = color.New(color.FgGreen).SprintFunc()
	// var HighlightYellow = color.New(color.FgYellow).SprintFunc()

	//	RunTests()
	//
	_, _, err := StandupTestDatabaseService()
	if err != nil {
		ColorLogger.Info("Failed to setup test database service", zap.Error(err))
	}
	// StandupTestDatabaseService()
	// ColorLogger.Info("Starting Test Runners", zap.String(HighlightGreen("key"), HighlightYellow("value")))
}
