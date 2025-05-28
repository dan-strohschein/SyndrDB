package directors

import (
	"fmt"
	"strings"
	"syndrdb/src/engine"

	"go.uber.org/zap"
)

func CommandDirector(databaseName string, serviceManager ServiceManager, command string, logger *zap.SugaredLogger) (interface{}, error) {
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon if present
	commandParts := strings.Split(command, " ")
	result := ""

	if strings.HasPrefix(strings.ToLower(command), "select") {
		// Parse SELECT command
		//dbCommand, err := engine.ParseSelectCommand(command)
		switch strings.ToLower(commandParts[1]) {
		case "databases":
			if len(commandParts) < 3 {
				return nil, fmt.Errorf("SELECT DATABASES requires the spec 'FROM Default'")
			}
			if strings.EqualFold(commandParts[3], "DEFAULT") {
				databases := serviceManager.DatabaseService.ListDatabases()

				if len(databases) == 0 {
					//fmt.Print("No databases found.\n")
					databases = make([]*engine.Database, 0)
				}

				cmdResponse := &engine.CommandResponse{
					ResultCount: len(databases),
					Result:      databases,
				}
				return &cmdResponse, nil
			}

		}
		return nil, nil
	}

	if strings.HasPrefix(strings.ToLower(command), "create") {

		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := engine.ParseCreateDatabaseCommand(command, logger)
			if err != nil {
				return nil, err
			}

			// Check if the database already exists
			existingDB, err := serviceManager.DatabaseService.GetDatabaseByName(dbCommand.DatabaseName)
			if err == nil {
				return nil, fmt.Errorf("database '%s' already exists", existingDB.Name)
			}

			//Validate the database name with a regex
			if !engine.IsValidDatabaseName(dbCommand.DatabaseName) {
				return nil, fmt.Errorf("invalid database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", dbCommand.DatabaseName)
			}
			// Execute the database command
			err = serviceManager.DatabaseService.AddDatabase(*dbCommand)
			if err != nil {
				return nil, fmt.Errorf("error creating database: %v", err)
			}
			result = fmt.Sprintf("Database '%s' created successfully.", dbCommand.DatabaseName)
			cmdResponse := &engine.CommandResponse{
				ResultCount: 1,
				Result:      result,
			}
			return cmdResponse, nil
		case "bundle":
			bundleCmd, err := engine.ParseCreateBundleCommand(command)
			if err != nil {
				return nil, fmt.Errorf("error parsing bundle command: %v", err)
			}

			//Check if the bundle already exists
			existingBundle, err := serviceManager.BundleService.GetBundleByName(bundleCmd.BundleName)
			if err == nil {
				return nil, fmt.Errorf("bundle '%s' already exists", existingBundle.Name)
			}

			// Get database object by name
			database, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
			if err != nil {
				return nil, fmt.Errorf("error retrieving database '%s': %v", databaseName, err)
			}

			// Add the bundle to the database
			err = serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, *bundleCmd)
			if err != nil {
				return nil, fmt.Errorf("error creating bundle: %v", err)
			}

			// Return the response
			result = fmt.Sprintf("Bundle '%s' created successfully in database '%s'.", bundleCmd.BundleName, databaseName)
			cmdResponse := &engine.CommandResponse{
				ResultCount: 1,
				Result:      result,
			}
			return cmdResponse, nil

		case "user":
			// ParseCreateRelationshipCommand(command)
		default:

			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse Add Document command
	if strings.HasPrefix(strings.ToLower(command), "add") {
		switch strings.ToLower(commandParts[1]) {
		case "document":
			if len(commandParts) < 4 {
				return nil, fmt.Errorf("ADD DOCUMENT requires the spec 'TO <bundle_name>'")
			}
			bundleName := commandParts[3]
			// Parse the document command
			docCommand, err := engine.ParseAddDocumentCommand(command, bundleName)
			if err != nil {
				return nil, fmt.Errorf("error parsing add document command: %v", err)
			}
			// Get the bundle by name
			bundle, err := serviceManager.BundleService.GetBundleByName(bundleName)
			if err != nil {
				return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
			}
			// Add the document to the bundle
			err = serviceManager.BundleService.AddDocumentToBundle(bundle, docCommand)
			if err != nil {
				return nil, fmt.Errorf("error adding document to bundle '%s': %v", bundleName, err)
			}
			result = fmt.Sprintf("Document added successfully to bundle '%s'.", bundleName)
			cmdResponse := &engine.CommandResponse{
				ResultCount: 1,
				Result:      result,
			}
			return cmdResponse, nil
		}
	}

	// Parse UPDATE  command
	if strings.HasPrefix(strings.ToLower(command), "update") {
		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := engine.ParseUpdateDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.UpdateDatabase(*dbCommand)
		case "bundle":
			engine.ParseUpdateBundleCommand(command)
		case "user":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse DELETE  command
	if strings.HasPrefix(strings.ToLower(command), "delete") {

		switch strings.ToLower(commandParts[1]) {
		case "database":
			dbCommand, err := engine.ParseDeleteDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.DeleteDatabase(dbCommand.DatabaseName)
		case "bundle":
			engine.ParseDeleteBundleCommand(command)
		case "user":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	return &result, nil
}
