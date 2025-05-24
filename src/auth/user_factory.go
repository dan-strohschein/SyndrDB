package auth

import "syndrdb/src/helpers"

type UserFactoryImpl struct {
	// TODO Add configuration fields here if needed
	// For example:
	defaultDataDir string
}

func NewUserFactory() UserFactory {
	return &UserFactoryImpl{
		// Initialize with default values if needed
	}
}

func (f *UserFactoryImpl) NewUserStruct(userName string, password string) *NewUser {
	return &NewUser{
		UserID:   helpers.GenerateUUID(),
		Username: userName,
		Password: password,
	}
}
