package directors

import (
	"fmt"
	"strings"
	"syndrdb/src/engine"
)

func CommandDirector(service *DatabaseService, command string) (interface{}, error) {
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon if present
	commandParts := strings.Split(command, " ")
	result := ""

	if strings.HasPrefix(command, "SELECT") {
		// Parse SELECT command
		//dbCommand, err := engine.ParseSelectCommand(command)
		switch commandParts[1] {
		case "DATABASES":
			if len(commandParts) < 3 {
				return nil, fmt.Errorf("SELECT DATABASES requires the spec 'FROM Default'")
			}
			if strings.EqualFold(commandParts[3], "DEFAULT") {
				// TODO get a json list of databases
				databases := service.ListDatabases()

				//renderedDatabases, err := json.Marshal(databases)
				// if err != nil {
				// 	return nil, fmt.Errorf("failed to render databases: %w", err)
				// }
				if len(databases) == 0 {
					//fmt.Print("No databases found.\n")
					databases = make([]*engine.Database, 0)
				}

				return &databases, nil

			}

		}
		return nil, nil
	}

	if strings.HasPrefix(command, "CREATE") {

		switch commandParts[1] {
		case "DATABASE":
			dbCommand, err := engine.ParseCreateDatabaseCommand(command)
			if err != nil {
				return nil, err
			}
			// Execute the database command
			service.AddDatabase(*dbCommand)
		case "BUNDLE":
			engine.ParseCreateBundleCommand(command)
			//service.AddBundle(db, bundleCommand)
		case "USER":
			// ParseCreateRelationshipCommand(command)
		default:

			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse UPDATE BUNDLE command
	if strings.HasPrefix(command, "UPDATE") {
		switch commandParts[1] {
		case "DATABASE":
			dbCommand, err := engine.ParseUpdateDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			service.UpdateDatabase(*dbCommand)
		case "BUNDLE":
			engine.ParseUpdateBundleCommand(command)
		case "USER":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	// Parse DELETE BUNDLE command
	if strings.HasPrefix(command, "DELETE") {

		switch commandParts[1] {
		case "DATABASE":
			dbCommand, err := engine.ParseDeleteDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			service.DeleteDatabase(dbCommand.DatabaseName)
		case "BUNDLE":
			engine.ParseDeleteBundleCommand(command)
		case "USER":
			// ParseCreateRelationshipCommand(command)
		default:
			return &result, fmt.Errorf("unknown command format: %s", command)
		}
		return &result, nil
	}

	return &result, nil
}
