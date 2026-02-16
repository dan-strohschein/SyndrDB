package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genCreateUser() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	password := fmt.Sprintf("pass%d!", g.rng.Intn(9999))

	nl := fmt.Sprintf("Create a new user %s with a secure password", username)

	stmt := ir.Statement{
		StatementType: "createUser",
		CreateUser:    &ir.CreateUserStmt{Username: username, Password: password},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genDeleteUser() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	force := g.rng.Intn(3) == 0

	nl := fmt.Sprintf("Delete user %s", username)
	if force {
		nl += " (force)"
	}

	stmt := ir.Statement{
		StatementType: "deleteUser",
		DeleteUser:    &ir.DeleteUserStmt{Username: username, Force: force},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genGrantPermission() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	perm := PermissionValues[g.rng.Intn(len(PermissionValues))]

	nl := fmt.Sprintf("Grant %s permission to user %s", perm, username)

	stmt := ir.Statement{
		StatementType:   "grantPermission",
		GrantPermission: &ir.GrantPermissionStmt{Permission: perm, Username: username},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genGrantRole() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	role := RoleNames[g.rng.Intn(len(RoleNames))]

	nl := fmt.Sprintf("Grant the %s role to user %s", role, username)

	stmt := ir.Statement{
		StatementType: "grantRole",
		GrantRole:     &ir.GrantRoleStmt{RoleName: role, Username: username},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRevokePermission() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	perm := PermissionValues[g.rng.Intn(len(PermissionValues))]

	nl := fmt.Sprintf("Revoke %s permission from user %s", perm, username)

	stmt := ir.Statement{
		StatementType:    "revokePermission",
		RevokePermission: &ir.RevokePermissionStmt{Permission: perm, Username: username},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRevokeRole() (*ir.TrainingExample, error) {
	username := UserNames[g.rng.Intn(len(UserNames))]
	role := RoleNames[g.rng.Intn(len(RoleNames))]

	nl := fmt.Sprintf("Revoke the %s role from user %s", role, username)

	stmt := ir.Statement{
		StatementType: "revokeRole",
		RevokeRole:    &ir.RevokeRoleStmt{RoleName: role, Username: username},
	}
	return g.makeExample(nl, stmt)
}
