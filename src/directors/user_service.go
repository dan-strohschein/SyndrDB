package directors

import (
	"log"
	"syndrdb/src/auth"
	"syndrdb/src/settings"
)

type UserService struct {
	// Add fields for managing users
	store    *auth.UserStore
	factory  auth.UserFactory
	settings *settings.Arguments
	users    map[string]*auth.User
}

func NewUserService(store *auth.UserStore, factory auth.UserFactory, settings *settings.Arguments) *UserService {
	service := &UserService{
		store:    store,
		factory:  factory,
		settings: settings,
		users:    make(map[string]*auth.User),
	}

	// Load existing users
	err := store.Load()
	if err != nil {
		log.Printf("Warning: Error loading database server users: %v", err)
	} else {

		log.Printf("users service loaded %d user", len(service.users))
	}

	return service
}

func (s *UserService) AddUser(userName string, password string) error {
	// Check if the user already exists
	if _, err := s.GetUserByName(userName); err == nil {
		return auth.ErrUserAlreadyExists
	}

	// Create a new user
	user := s.factory.NewUserStruct(userName, password)

	// Add the user to the store
	storedUser, err := s.store.AddUser(*user)
	if err != nil {
		return err
	}

	s.users[userName] = storedUser

	return nil
}

func (s *UserService) GetUserByName(userName string) (*auth.User, error) {
	user, err := s.store.GetUserByName(userName)
	if err != nil {
		return nil, err
	}

	return user, nil
}
func (s *UserService) GetAllUsers() ([]*auth.User, error) {
	users, err := s.store.GetAllUsers()
	if err != nil {
		return nil, err
	}

	return users, nil
}
func (s *UserService) UpdateUser(userName string, password string) error {
	updatedUser := s.factory.NewUserStruct(userName, password)

	// Save the updated user to the store
	err := s.store.UpdateUser(*updatedUser)
	if err != nil {
		return err
	}

	return nil
}
func (s *UserService) DeleteUser(userName string) error {

	// Delete the user from the store
	err := s.store.RemoveUser(userName)
	if err != nil {
		return err
	}

	delete(s.users, userName)

	return nil
}
