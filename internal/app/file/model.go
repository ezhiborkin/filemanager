package file

import (
	"mime/multipart"

	"github.com/minio/minio-go/v7"
)

type FileNames struct {
	Id string `json:"id"`
}

type Rename struct {
	Old string `json:"old"`
	New string `json:"new"`
}

type Move struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

type Upload struct {
	Name string
	Size int64
	Type string
	Data multipart.File
}

type File struct {
	Id   string        `json:"id"`
	Size int64         `json:"size"`
	Type string        `json:"type"`
	Obj  *minio.Object `json:"-"`
}

type Dir struct {
	SubDirs []string
	Files   []string
}

type SubDir struct {
	Name    string   `json:"name"`
	SubDirs []SubDir `json:"subDirs"`
	Files   []string `json:"files"`
}

type Repos struct {
	Id   int    `json:"id"`
	Name string `json:"repo"`
}

type RepoFiles struct {
	Id   int    `json:"id"`
	Name string `json:"path"`
}

type RepoPermsId struct {
	Id         int    `json:"id"`
	RoleTitle  string `json:"role_title"`
	Path       string `json:"path"`
	Permission string `json:"permission"`
}

type RepoPerms struct {
	RoleTitle  string `json:"role_title"`
	Path       string `json:"path"`
	Permission string `json:"permission"`
}
