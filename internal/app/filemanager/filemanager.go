package filemanager

import (
	"database/sql"
	"files_test_rus/internal/app/file/store/minio"
	"fmt"
	"net/http"
	"os"

	"github.com/sirupsen/logrus"
)

var (
	endpoint        = os.Getenv("ENDPOINT")
	accessKeyID     = os.Getenv("ACCESS_KEY_ID")
	secretAccessKey = os.Getenv("SECRET_ACCESS_KEY")
)

func Start(config *Config, logger *logrus.Logger) error {
	db, err := newDB(config.DatabaseURL)
	if err != nil {
		return err
	}

	defer db.Close()

	client, err := minio.NewClient(db, endpoint, accessKeyID, secretAccessKey, logger)
	if err != nil {
		return fmt.Errorf("failed to create minio client. err: %w", err)
	}

	srv := newServer(client, logger)

	return http.ListenAndServe(config.BindAddr, srv)
}

func newDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
