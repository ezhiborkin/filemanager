package minio

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	model "files_test_rus/internal/app/file"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
)

var (
	bucket      = "roflan"
	DatabaseURL = "host=localhost dbname=restapi_dev sslmode=disable"
)

type Client struct {
	db     *sql.DB
	logger *logrus.Logger
	client *minio.Client
	bucket string
}

func NewClient(db *sql.DB, endpoint, accessKey, secretKey string, logger *logrus.Logger) (*Client, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client. err: %w", err)
	}

	return &Client{
		logger: logger,
		bucket: bucket,
		client: client,
		db:     db,
	}, nil
}

func (c *Client) GetFile(ctx context.Context, fileName string) (*minio.Object, error) {
	c.logger.Infof("DOWNLOAD A FILE %s", fileName)
	role_title := ctx.Value("role")

	var title string
	c.logger.Info(fileName)
	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	var permission string

	finalName := strings.Split(fileName, "/")
	finalName = RemoveIndex(finalName, 0)
	name := strings.Join(finalName, "/")
	name = strings.TrimSuffix(name, "/")
	// newName := name
	repName := finalName[0]

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[2] == 'd' {
						obj, err := c.client.GetObject(ctx, c.bucket, fileName, minio.GetObjectOptions{})
						if err != nil {
							return nil, err
						}
						return obj, nil
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
			} else {
				if permission[2] == 'd' {
					obj, err := c.client.GetObject(ctx, c.bucket, fileName, minio.GetObjectOptions{})
					if err != nil {
						return nil, err
					}
					return obj, nil
				}
			}
		}
	} else {
		c.logger.Info("ADM ROLE")
		obj, err := c.client.GetObject(ctx, c.bucket, fileName, minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	return nil, nil
}

func (c *Client) GetRepositories(ctx context.Context) (*[]model.Repos, error) {
	var title string

	role_title := ctx.Value("role")
	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		rows, err := c.db.Query("SELECT id, repo FROM repositories")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var reposList []model.Repos

		for rows.Next() {
			var reposListEl model.Repos
			if err := rows.Scan(&reposListEl.Id, &reposListEl.Name); err != nil {
				return &reposList, err
			}
			reposList = append(reposList, reposListEl)
		}

		if err = rows.Err(); err != nil {
			return &reposList, err
		}

		return &reposList, err
	}

	return nil, nil
}

func (c *Client) RemoveRepositoryPerms(ctx context.Context, repoName string, rp model.RepoPerms) error {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		query := fmt.Sprintf("DELETE FROM %s WHERE role_title = '%s' AND path = '%s' AND permission = '%s'", repoName+"_perms", rp.RoleTitle, rp.Path, rp.Permission)
		if _, err := c.db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) EditRepositoryPerms(ctx context.Context, repoName string, rp model.RepoPermsId) error {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		query := fmt.Sprintf("UPDATE %s SET role_title = '%s', path = '%s', permission = '%s' WHERE id = '%s'", repoName+"_perms", rp.RoleTitle, rp.Path, rp.Permission, strconv.Itoa(rp.Id))
		if _, err := c.db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetRepositoryFiles(ctx context.Context, repoName string) (*[]model.RepoFiles, error) {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		query := fmt.Sprintf("SELECT id, path FROM %s", repoName)
		rows, err := c.db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var filesList []model.RepoFiles

		for rows.Next() {
			var filesListEl model.RepoFiles
			if err := rows.Scan(&filesListEl.Id, &filesListEl.Name); err != nil {
				return &filesList, err
			}

			filesList = append(filesList, filesListEl)
		}

		if err = rows.Err(); err != nil {
			return &filesList, err
		}

		return &filesList, err
	}

	return nil, nil
}

func (c *Client) AddRepositoryPerms(ctx context.Context, repoName string, repoPerm model.RepoPerms) error {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		query := fmt.Sprintf("INSERT INTO %s (role_title, path, permission) VALUES ('%s', '%s', '%s')", repoName+"_perms", repoPerm.RoleTitle, repoPerm.Path, repoPerm.Permission)
		if _, err := c.db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) GetRepositoryPerms(ctx context.Context, repoName string) (*[]model.RepoPermsId, error) {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		query := fmt.Sprintf("SELECT id, role_title, path, permission FROM %s", repoName+"_perms")
		rows, err := c.db.Query(query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var permsList []model.RepoPermsId

		for rows.Next() {
			var permsListEl model.RepoPermsId
			if err := rows.Scan(&permsListEl.Id, &permsListEl.RoleTitle, &permsListEl.Path, &permsListEl.Permission); err != nil {
				return &permsList, err
			}

			permsList = append(permsList, permsListEl)
		}

		if err = rows.Err(); err != nil {
			return &permsList, err
		}

		return &permsList, err
	}

	return nil, nil
}

func (c *Client) GetFiles(ctx context.Context) ([]model.SubDir, error) {
	objectCh := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    "backend",
		Recursive: true,
	})

	role_title := ctx.Value("role")
	c.logger.Info(role_title)

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)
	c.logger.Info(title)

	var folders []string
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}

		var permission string
		finalName := strings.Split(object.Key, "/")
		finalName = RemoveIndex(finalName, 0)
		name := strings.Join(finalName, "/")
		name = strings.TrimSuffix(name, "/")
		newName := name
		repName := finalName[0]

		if title != "admin" {
			if len(strings.Split(name, "/")) > 1 {
				for len(strings.Split(name, "/")) > 1 {
					query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
					if err := c.db.QueryRow(query).Scan(&permission); err != nil {
						if err == sql.ErrNoRows {
							c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
						}
					} else {
						if permission[0] == 'r' {
							c.logger.Info(newName)

							folders = append(folders, newName)
						}
						break
					}
					name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
				}
			} else {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						return nil, nil
					}
				} else {
					if permission[0] == 'r' {
						c.logger.Info(newName)
						folders = append(folders, newName)
					}
				}
			}
		} else {
			c.logger.Info("ADM ROLE")
			folders = append(folders, newName)
			c.logger.Info(newName)
		}

	}

	c.logger.Info(folders)
	subDir := toTree(folders)

	if subDir == nil {
		return nil, fmt.Errorf("empty data")
	}

	return subDir, nil
}

func (c *Client) UploadFile(ctx context.Context, fileName string, fileSize int64, reader io.Reader) error {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	role_title := ctx.Value("role")

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	var permission string

	name := strings.TrimPrefix(fileName, "backend/")
	repName := strings.Split(name, "/")[0]
	name = strings.TrimSuffix(name, "/")
	newName := name

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[1] == 'w' {
						exists, errBucketExists := c.client.BucketExists(ctx, c.bucket)
						if errBucketExists != nil || !exists {
							c.logger.Warnf("no bucket %s. creating new one...", c.bucket)
							err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
							if err != nil {
								return fmt.Errorf("failed to create new bucket. err: %w", err)
							}
						}

						c.logger.Debugf("put new object %s to bucket %s", fileName, c.bucket)
						_, err := c.client.PutObject(reqCtx, c.bucket, fileName, reader, fileSize,
							minio.PutObjectOptions{
								ContentType: "application/octet-stream",
							})
						if err != nil {
							return fmt.Errorf("failed to upload file. err: %w", err)
						}

						query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
						if err := c.db.QueryRow(query).Err(); err != nil {
							c.logger.Info(err)
						}
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
			} else {
				if permission[0] == 'r' {
					exists, errBucketExists := c.client.BucketExists(ctx, c.bucket)
					if errBucketExists != nil || !exists {
						c.logger.Warnf("no bucket %s. creating new one...", c.bucket)
						err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
						if err != nil {
							return fmt.Errorf("failed to create new bucket. err: %w", err)
						}
					}

					c.logger.Debugf("put new object %s to bucket %s", fileName, c.bucket)
					_, err := c.client.PutObject(reqCtx, c.bucket, fileName, reader, fileSize,
						minio.PutObjectOptions{
							ContentType: "application/octet-stream",
						})
					if err != nil {
						return fmt.Errorf("failed to upload file. err: %w", err)
					}

					query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
					if err := c.db.QueryRow(query).Err(); err != nil {
						c.logger.Info(err)
					}
				}
			}
		}
	} else {
		c.logger.Info("ADM ROLE")
		exists, errBucketExists := c.client.BucketExists(ctx, c.bucket)
		if errBucketExists != nil || !exists {
			c.logger.Warnf("no bucket %s. creating new one...", c.bucket)
			err := c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
			if err != nil {
				return fmt.Errorf("failed to create new bucket. err: %w", err)
			}
		}

		c.logger.Debugf("put new object %s to bucket %s", fileName, c.bucket)
		_, err := c.client.PutObject(reqCtx, c.bucket, fileName, reader, fileSize,
			minio.PutObjectOptions{
				ContentType: "application/octet-stream",
			})
		if err != nil {
			return fmt.Errorf("failed to upload file. err: %w", err)
		}

		query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
		if err := c.db.QueryRow(query).Err(); err != nil {
			c.logger.Info(err)
		}
	}

	return nil
}

func (c *Client) RemoveFile(ctx context.Context, fileName string) error {
	c.logger.Infof("name: %s", fileName)

	role_title := ctx.Value("role")

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	var permission string

	name := strings.TrimPrefix(fileName, "backend/")
	repName := strings.Split(name, "/")[0]
	name = strings.TrimPrefix(name, "/")
	newName := name
	c.logger.Info(newName)

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[1] == 'w' {
						if err := c.client.RemoveObject(ctx, c.bucket, fileName, minio.RemoveObjectOptions{}); err != nil {
							return fmt.Errorf("failed to delete file. err: %w", err)
						}

						query := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName, newName)
						if err := c.db.QueryRow(query).Err(); err != nil {
							if err == sql.ErrNoRows {
								return errors.New("failed to delete file not found")
							}
							return err
						}

						query2 := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName+"_perms", newName)
						if err := c.db.QueryRow(query2).Err(); err != nil {
							if err == sql.ErrNoRows {
								return errors.New("failed to delete file_perms not found")
							}
							return err
						}
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
			} else {
				if permission[1] == 'w' {
					if err := c.client.RemoveObject(ctx, c.bucket, fileName, minio.RemoveObjectOptions{}); err != nil {
						return fmt.Errorf("failed to delete file. err: %w", err)
					}

					query := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName, newName)
					if err := c.db.QueryRow(query).Err(); err != nil {
						if err == sql.ErrNoRows {
							return errors.New("failed to delete file not found")
						}
						return err
					}

					query2 := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName+"_perms", newName)
					if err := c.db.QueryRow(query2).Err(); err != nil {
						if err == sql.ErrNoRows {
							return errors.New("failed to delete file_perms not found")
						}
						return err
					}
				}
			}
		}
	} else {
		if err := c.client.RemoveObject(ctx, c.bucket, fileName, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete file. err: %w", err)
		}

		query := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName, newName)
		if err := c.db.QueryRow(query).Err(); err != nil {
			if err == sql.ErrNoRows {
				return errors.New("failed to delete file not found")
			}
			return err
		}

		query2 := fmt.Sprintf("DELETE FROM %s WHERE path = '%s'", repName+"_perms", newName)
		if err := c.db.QueryRow(query2).Err(); err != nil {
			if err == sql.ErrNoRows {
				return errors.New("failed to delete file_perms not found")
			}
			return err
		}
	}

	return nil
}

func (c *Client) RenameDir(ctx context.Context, old, new string) error {
	src := minio.CopySrcOptions{
		Bucket: c.bucket,
		Object: old,
	}

	dst := minio.CopyDestOptions{
		Bucket: c.bucket,
		Object: new,
	}

	role_title := ctx.Value("role")

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	var permission string

	oldName := strings.TrimPrefix(old, "backend/")
	name2 := strings.Split(oldName, "/")
	name2 = RemoveIndex(name2, 0)
	finalOldName := strings.Join(name2, "/")
	finalOldName = strings.TrimSuffix(finalOldName, "/")

	name := strings.TrimPrefix(new, "backend/")
	newName := strings.Split(name, "/")
	repName := newName[0]
	newName = RemoveIndex(newName, 0)
	finalName := strings.Join(newName, "/")
	finalName = strings.TrimSuffix(finalName, "/")

	fName := strings.Split(finalName, "/")
	fName = RemoveIndex(fName, len(fName)-1)
	updatedName := strings.Join(fName, "/")

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[0] == 'r' {
						_, err := c.client.CopyObject(ctx, dst, src)
						if err != nil {
							return fmt.Errorf("failed to rename file. err: %w", err)
						}

						if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
							return err
						}

						query1 := fmt.Sprintf("UPDATE %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName, finalOldName, updatedName, finalOldName)
						if _, err := c.db.Exec(query1); err != nil {
							return err
						}

						query := fmt.Sprintf("UPDATE  %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName+"_dirs", finalOldName, updatedName, finalOldName)
						if _, err := c.db.Exec(query); err != nil {
							return err
						}
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
			} else {
				if permission[0] == 'r' {
					_, err := c.client.CopyObject(ctx, dst, src)
					if err != nil {
						return fmt.Errorf("failed to rename file. err: %w", err)
					}

					if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
						return err
					}

					query1 := fmt.Sprintf("UPDATE %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName, finalOldName, updatedName, finalOldName)
					if _, err := c.db.Exec(query1); err != nil {
						return err
					}

					query := fmt.Sprintf("UPDATE  %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName+"_dirs", finalOldName, updatedName, finalOldName)
					if _, err := c.db.Exec(query); err != nil {
						return err
					}
				}
			}
		}
	} else {
		c.logger.Info("ADM ROLE")
		_, err := c.client.CopyObject(ctx, dst, src)
		if err != nil {
			return fmt.Errorf("failed to rename file. err: %w", err)
		}

		if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
			return err
		}

		query1 := fmt.Sprintf("UPDATE %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName, finalOldName, updatedName, finalOldName)
		if _, err := c.db.Exec(query1); err != nil {
			return err
		}

		query := fmt.Sprintf("UPDATE  %s SET path = REGEXP_REPLACE (path, '^%s($|/)', '%s') WHERE path ~ '^%s($|/)'", repName+"_dirs", finalOldName, updatedName, finalOldName)
		if _, err := c.db.Exec(query); err != nil {
			return err
		}
	}

	//

	return nil
}

func (c *Client) RenameFile(ctx context.Context, old, new string) error {
	src := minio.CopySrcOptions{
		Bucket: c.bucket,
		Object: old,
	}

	dst := minio.CopyDestOptions{
		Bucket: c.bucket,
		Object: new,
	}

	role_title := ctx.Value("role")

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	oldName := strings.TrimPrefix(old, "backend/")
	repName := strings.Split(oldName, "/")[0]
	oldName = strings.TrimSuffix(oldName, "/")
	newName := strings.TrimPrefix(new, "backend/")
	newName = strings.TrimSuffix(newName, "/")
	coco := strings.Split(newName, "/")
	s := RemoveIndex(coco, len(coco)-2)
	name := strings.Join(s, "/")
	newName = name
	name = oldName

	var permission string

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[1] == 'w' {
						_, err := c.client.CopyObject(ctx, dst, src)
						if err != nil {
							return fmt.Errorf("failed to rename file. err: %w", err)
						}

						if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
							return err
						}

						var id, idP string

						query1 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName, oldName)
						if err := c.db.QueryRow(query1).Scan(&id); err != nil {
							return err
						}

						query := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName, newName, id)
						if err := c.db.QueryRow(query).Err(); err != nil {
							return err
						}

						query3 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName+"_perms", oldName)
						if err := c.db.QueryRow(query3).Scan(&idP); err != nil {
							if err == sql.ErrNoRows {
								return nil
							}
							return err
						}

						query2 := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName+"_perms", newName, idP)
						if err := c.db.QueryRow(query2).Err(); err != nil {
							if err == sql.ErrNoRows {
								return nil
							}
							return err
						}
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
			} else {
				if permission[1] == 'w' {
					_, err := c.client.CopyObject(ctx, dst, src)
					if err != nil {
						return fmt.Errorf("failed to rename file. err: %w", err)
					}

					if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
						return err
					}

					var id, idP string

					query1 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName, oldName)
					if err := c.db.QueryRow(query1).Scan(&id); err != nil {
						return err
					}

					query := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName, newName, id)
					if err := c.db.QueryRow(query).Err(); err != nil {
						return err
					}

					query3 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName+"_perms", oldName)
					if err := c.db.QueryRow(query3).Scan(&idP); err != nil {
						if err == sql.ErrNoRows {
							return nil
						}
						return err
					}

					query2 := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName+"_perms", newName, idP)
					if err := c.db.QueryRow(query2).Err(); err != nil {
						if err == sql.ErrNoRows {
							return nil
						}
						return err
					}
				}
			}
		}
	} else {
		c.logger.Info("ADM ROLE")
		_, err := c.client.CopyObject(ctx, dst, src)
		if err != nil {
			return fmt.Errorf("failed to rename file. err: %w", err)
		}

		if err := c.client.RemoveObject(ctx, c.bucket, old, minio.RemoveObjectOptions{}); err != nil {
			return err
		}

		var id, idP string

		query1 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName, oldName)
		if err := c.db.QueryRow(query1).Scan(&id); err != nil {
			return err
		}

		query := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName, newName, id)
		if err := c.db.QueryRow(query).Err(); err != nil {
			return err
		}

		query3 := fmt.Sprintf("SELECT id FROM %s WHERE path = '%s'", repName+"_perms", oldName)
		if err := c.db.QueryRow(query3).Scan(&idP); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		query2 := fmt.Sprintf("UPDATE %s SET path = '%s' WHERE id = %s", repName+"_perms", newName, idP)
		if err := c.db.QueryRow(query2).Err(); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
	}

	//

	return nil
}

func (c *Client) CreateDirectory(ctx context.Context, dir string) error {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	name := strings.TrimPrefix(dir, "backend/")
	name = strings.TrimSuffix(name, "/")
	repName := strings.Split(name, "/")[0]
	newName := name

	var permission string

	if title != "admin" {
		if len(strings.Split(name, "/")) > 1 {
			for len(strings.Split(name, "/")) > 1 {
				query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
				if err := c.db.QueryRow(query).Scan(&permission); err != nil {
					if err == sql.ErrNoRows {
						c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
					}
				} else {
					if permission[0] == 'r' {
						if _, err := c.client.PutObject(ctx, c.bucket, dir+"/", nil, 0, minio.PutObjectOptions{}); err != nil {
							return err
						}

						query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
						if _, err := c.db.Exec(query); err != nil {
							return err
						}
					}
					break
				}
				name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
			}
		} else {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
			} else {
				if permission[0] == 'r' {
					if _, err := c.client.PutObject(ctx, c.bucket, dir+"/", nil, 0, minio.PutObjectOptions{}); err != nil {
						return err
					}

					query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
					if _, err := c.db.Exec(query); err != nil {
						return err
					}
				}
			}
		}
	} else {
		c.logger.Info("ADM ROLE")
		if _, err := c.client.PutObject(ctx, c.bucket, dir+"/", nil, 0, minio.PutObjectOptions{}); err != nil {
			return err
		}

		query := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", repName, newName)
		if _, err := c.db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) CreateRepository(ctx context.Context, dir string) error {
	var title string

	role_title := ctx.Value("role")

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if title == "admin" {
		if _, err := c.client.PutObject(ctx, c.bucket, dir+"/", nil, 0, minio.PutObjectOptions{}); err != nil {
			return err
		}

		name := strings.TrimPrefix(dir, "backend/")
		name = strings.TrimSuffix(name, "/")

		if _, err := c.db.Exec("INSERT INTO repositories (repo) VALUES ($1)", name); err != nil {
			return err
		}

		kek := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigserial not null primary key, path varchar not null unique)", name)
		if _, err := c.db.Exec(kek); err != nil {
			return err
		}

		kek0 := fmt.Sprintf("INSERT INTO %s (path) VALUES ('%s')", name, name)
		if _, err := c.db.Exec(kek0); err != nil {
			return err
		}

		kek2 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigserial not null primary key, role_title varchar not null, path varchar not null, permission varchar not null)", name+"_perms")
		if _, err := c.db.Exec(kek2); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) RenameDirectory(ctx context.Context, old, new string) error {
	objectCh := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    old,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		if err := c.RenameDir(ctx, object.Key,
			strings.Replace(
				object.Key,
				path.Dir(strings.TrimRight(object.Key, "/")),
				new,
				1,
			),
		); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) RemoveDirectory(ctx context.Context, dir string) error {

	var permission string

	role_title := ctx.Value("role")

	c.logger.Infof("Removed directory: %s", dir)

	name := strings.TrimPrefix(dir, "backend/")
	coco := strings.Split(name, "/")
	repName := coco[0]
	dirName := strings.Join(coco, "/")
	dirName = strings.TrimSuffix(dirName, "/")

	var title string

	c.db.QueryRow(
		"SELECT title FROM roles WHERE id = $1",
		role_title,
	).Scan(&title)

	if len(strings.Split(name, "/")) > 1 {
		for len(strings.Split(name, "/")) > 1 {
			query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
			if err := c.db.QueryRow(query).Scan(&permission); err != nil {
				if err == sql.ErrNoRows {
					c.logger.Infof("NO PERMISSION FOUND FOR PATH = %s", name)
				}
			} else {
				if permission[1] == 'w' {
					//
					objectsCh := make(chan minio.ObjectInfo)

					go func() {
						defer close(objectsCh)

						for object := range c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
							Prefix:    dir,
							Recursive: true,
						}) {
							if object.Err != nil {
								c.logger.Errorf("list of objects error: %v", object.Err.Error())
							} else {
								objectsCh <- object
							}
						}
					}()

					for rErr := range c.client.RemoveObjects(ctx, c.bucket, objectsCh, minio.RemoveObjectsOptions{
						GovernanceBypass: true,
					}) {
						c.logger.Errorf("remove object error %v", rErr.Err.Error())
					}

					query2 := fmt.Sprintf("DELETE FROM %s WHERE path ~ '^%s($|/)'", repName, dirName)
					if _, err := c.db.Query(query2); err != nil {
						return err
					}

					query3 := fmt.Sprintf("DELETE FROM %s WHERE path ~ '^%s($|/)'", repName+"_perms", dirName)
					if _, err := c.db.Query(query3); err != nil {
						return err
					}
				}
				break
			}
			name = strings.Join(RemoveIndex(strings.Split(name, "/"), len(strings.Split(name, "/"))-1), "/")
		}
	} else {
		query := fmt.Sprintf("SELECT permission FROM %s WHERE path = '%s' AND role_title = '%s'", repName+"_perms", name, title)
		if err := c.db.QueryRow(query).Scan(&permission); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
		} else {
			if permission[1] == 'w' {
				//
				objectsCh := make(chan minio.ObjectInfo)

				go func() {
					defer close(objectsCh)

					for object := range c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
						Prefix:    dir,
						Recursive: true,
					}) {
						if object.Err != nil {
							c.logger.Errorf("list of objects error: %v", object.Err.Error())
						} else {
							objectsCh <- object
						}
					}
				}()

				for rErr := range c.client.RemoveObjects(ctx, c.bucket, objectsCh, minio.RemoveObjectsOptions{
					GovernanceBypass: true,
				}) {
					c.logger.Errorf("remove object error %v", rErr.Err.Error())
				}

				query2 := fmt.Sprintf("DELETE FROM %s WHERE path ~ '^%s($|/)'", repName, dirName)
				if _, err := c.db.Query(query2); err != nil {
					return err
				}

				query3 := fmt.Sprintf("DELETE FROM %s WHERE path ~ '^%s($|/)'", repName+"_perms", dirName)
				if _, err := c.db.Query(query3); err != nil {
					return err
				}
			}
		}
	}

	//

	return nil
}

func toTree(objectKeys []string) []model.SubDir {
	dirsMap := make(map[string]model.Dir)

	if len(objectKeys) == 1 {
		key := objectKeys[0]
		if i := strings.IndexByte(key, '/'); i > 0 {
			nameDir := key[:i]
			subPath := key[i+1:]

			var (
				f, sb []string
				err   error
			)
			subPath, err = url.QueryUnescape(subPath)
			if err != nil {

			} else {
				if isFile(subPath) {
					f = []string{subPath}
				} else {
					sb = []string{subPath}
				}
			}

			dirsMap[nameDir] = model.Dir{
				SubDirs: sb,
				Files:   f,
			}
		} else {
			dirsMap[key] = model.Dir{}
		}
	} else {
		for _, key := range objectKeys {
			if i := strings.IndexByte(key, '/'); i > 0 {
				nameDir := key[:i]
				subPath := key[i+1:]
				sb := dirsMap[nameDir].SubDirs
				f := dirsMap[nameDir].Files

				var err error
				subPath, err = url.QueryUnescape(subPath)
				if err != nil {

				} else {
					if isFile(subPath) {
						f = append(f, subPath)
					} else {
						sb = append(sb, subPath)
					}
				}

				dirsMap[nameDir] = model.Dir{
					SubDirs: sb,
					Files:   f,
				}
			} else {
				dirsMap[key] = model.Dir{}
			}
		}
	}

	subDirs := make([]model.SubDir, len(dirsMap))
	i := 0
	for k, v := range dirsMap {
		subDirs[i] = model.SubDir{
			Name:    k,
			SubDirs: toTree(v.SubDirs),
			Files:   v.Files,
		}
		i++
	}

	sort.Slice(subDirs, func(i, j int) bool {
		return subDirs[j].Name > subDirs[i].Name
	})

	return subDirs
}

func isFile(path string) bool {
	if strings.Contains(path, ".") && !strings.Contains(path, "/") {
		return true
	}
	return false
}

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}
