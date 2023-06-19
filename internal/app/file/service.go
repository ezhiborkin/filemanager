package file

import (
	"context"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

type service struct {
	storage Client
	logger  *logrus.Logger
}

func NewService(minioStorage Client, logger *logrus.Logger) (Service, error) {
	return &service{
		storage: minioStorage,
		logger:  logger,
	}, nil
}

type Service interface {
	GetFile(context.Context, string) (*File, error)
	UploadFile(context.Context, *Upload) error
	RemoveFile(context.Context, string) error
	RenameFile(context.Context, Rename) error
	MoveFile(context.Context, Move) error

	GetFiles(context.Context) ([]SubDir, error)

	CreateDirectory(context.Context, string) error
	RenameDirectory(context.Context, Rename) error
	MoveDirectory(context.Context, Move) error
	RemoveDirectory(context.Context, string) error

	CreateRepository(context.Context, string) error
	GetRepositories(context.Context) (*[]Repos, error)
	GetRepositoryFiles(context.Context, string) (*[]RepoFiles, error)
	GetRepositoryPerms(context.Context, string) (*[]RepoPermsId, error)
	AddRepositoryPerms(context.Context, string, RepoPerms) error
	EditRepositoryPerms(context.Context, string, RepoPermsId) error
	RemoveRepositoryPerms(context.Context, string, RepoPerms) error
}

func (s *service) RemoveRepositoryPerms(ctx context.Context, repoName string, repoPerm RepoPerms) error {
	if err := s.storage.RemoveRepositoryPerms(ctx, repoName, repoPerm); err != nil {
		return fmt.Errorf("obj err: %v", err)
	}

	return nil
}

func (s *service) EditRepositoryPerms(ctx context.Context, repoName string, repoPerm RepoPermsId) error {
	if err := s.storage.EditRepositoryPerms(ctx, repoName, repoPerm); err != nil {
		return fmt.Errorf("obj err: %v", err)
	}

	return nil
}

func (s *service) AddRepositoryPerms(ctx context.Context, repoName string, repoPerm RepoPerms) error {
	if err := s.storage.AddRepositoryPerms(ctx, repoName, repoPerm); err != nil {
		return fmt.Errorf("obj err: %v", err)
	}

	return nil
}

func (s *service) GetRepositoryPerms(ctx context.Context, repoName string) (*[]RepoPermsId, error) {
	object, err := s.storage.GetRepositoryPerms(ctx, repoName)
	if err != nil {
		return nil, fmt.Errorf("obj err: %v", err)
	}
	return object, nil
}

func (s *service) GetRepositoryFiles(ctx context.Context, repoName string) (*[]RepoFiles, error) {
	object, err := s.storage.GetRepositoryFiles(ctx, repoName)
	if err != nil {
		return nil, fmt.Errorf("obj err: %v", err)
	}
	return object, nil
}

func (s *service) GetRepositories(ctx context.Context) (*[]Repos, error) {
	object, err := s.storage.GetRepositories(ctx)
	if err != nil {
		return nil, fmt.Errorf("obj err: %v", err)
	}
	return object, nil
}

func (s *service) GetFiles(ctx context.Context) ([]SubDir, error) {
	object, err := s.storage.GetFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("obj err: %v", err)
	}

	return object, nil
}

func (s *service) GetFile(ctx context.Context, filename string) (*File, error) {
	obj, err := s.storage.GetFile(ctx, filename)
	if err != nil {
		return nil, err
	}

	objectInfo, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("obj.stat error: %v", err)
	}

	f := File{
		Id:   objectInfo.Key,
		Size: objectInfo.Size,
		Type: objectInfo.ContentType,
		Obj:  obj,
	}

	return &f, nil
}

func (s *service) UploadFile(ctx context.Context, file *Upload) error {
	if err := s.storage.UploadFile(ctx, file.Name, file.Size, file.Data); err != nil {
		return err
	}

	return nil
}

func (s *service) RemoveFile(ctx context.Context, fileName string) error {
	if err := s.storage.RemoveFile(ctx, fileName); err != nil {
		return err
	}

	return nil
}

func (s *service) RenameFile(ctx context.Context, fileName Rename) error {
	if err := s.storage.RenameFile(ctx, fileName.Old, fileName.New); err != nil {
		return err
	}

	return nil
}

func (s *service) MoveFile(ctx context.Context, param Move) error {
	if err := s.storage.RenameFile(ctx, param.Src, param.Dst); err != nil {
		return err
	}

	return nil
}

func (s *service) CreateDirectory(ctx context.Context, dir string) error {
	if err := s.storage.CreateDirectory(ctx, dir); err != nil {
		return err
	}

	return nil
}

func (s *service) CreateRepository(ctx context.Context, dir string) error {
	if err := s.storage.CreateRepository(ctx, dir); err != nil {
		return err
	}

	return nil
}

func (s *service) RenameDirectory(ctx context.Context, dirName Rename) error {
	if err := s.storage.RenameDirectory(ctx, dirName.Old, dirName.New); err != nil {
		return err
	}

	return nil
}

func (s *service) MoveDirectory(ctx context.Context, dirName Move) error {
	if err := s.storage.RenameDirectory(ctx, dirName.Src, dirName.Dst); err != nil {
		return err
	}

	return nil
}

func (s *service) RemoveDirectory(ctx context.Context, dirName string) error {
	if err := s.storage.RemoveDirectory(ctx, dirName); err != nil {
		return err
	}

	return nil
}
