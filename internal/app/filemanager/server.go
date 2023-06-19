package filemanager

import (
	"context"
	"encoding/json"
	"errors"
	storage "files_test_rus/internal/app/file"
	"files_test_rus/internal/app/file/store/minio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	model "files_test_rus/internal/app/file"

	"github.com/golang-jwt/jwt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type server struct {
	router  *mux.Router
	logger  *logrus.Logger
	service storage.Service
}

type Claims struct {
	RoleID string `json:"role_id"`
	Email  string `json:"email"`
	jwt.StandardClaims
}

var (
	jwtKey = []byte(os.Getenv("SECRET_KEY"))
)

const (
	prefix string = "backend/"
)

func newServer(client *minio.Client, logger *logrus.Logger) *server {
	service, err := storage.NewService(client, logger)
	if err != nil {
		logger.Fatal(err)
	}

	s := &server{
		router:  mux.NewRouter(),
		logger:  logger,
		service: service,
	}
	s.configureRouter()

	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *server) configureRouter() {
	s.router.Use(
		handlers.CORS(
			handlers.AllowedOrigins([]string{"http://localhost:3000"}),
			handlers.AllowedMethods([]string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"}),
			handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type"}),
			handlers.AllowCredentials(),
		),
		s.authorizeUser)

	// s.router.HandleFunc("/getallfiles", s.handleGetFiles()).Methods("GET", "OPTIONS")
	staticRouter := s.router.PathPrefix("/static").HandlerFunc(s.handleGetFile())
	staticRouter.HandlerFunc(s.handleGetFile()).Methods("GET", "OPTIONS")

	fileRouter := s.router.PathPrefix("/file").Subrouter()
	fileRouter.HandleFunc("/upload", s.handleUpload()).Methods("POST", "OPTIONS")
	fileRouter.HandleFunc("/remove", s.handleRemoveFile()).Methods("DELETE", "OPTIONS")
	fileRouter.HandleFunc("/rename", s.handleRenameFile()).Methods("POST", "OPTIONS")
	fileRouter.HandleFunc("/move", s.handleMoveFile()).Methods("POST", "OPTIONS")

	dirRouter := s.router.PathPrefix("/dir").Subrouter()
	dirRouter.HandleFunc("/create", s.handleCreateDirectory()).Methods("POST", "OPTIONS")
	dirRouter.HandleFunc("/rename", s.handleRenameDirectory()).Methods("POST", "OPTIONS")
	dirRouter.HandleFunc("/move", s.handleMoveDirectory()).Methods("POST", "OPTIONS")
	dirRouter.HandleFunc("/remove", s.handleRemoveDirectory()).Methods("DELETE", "OPTIONS")

	repRouter := s.router.PathPrefix("/rep").Subrouter()
	repRouter.HandleFunc("/create", s.handleCreateRepository()).Methods("POST", "OPTIONS")
	repRouter.HandleFunc("/get", s.handleGetRepositories()).Methods("GET", "OPTIONS")
	repRouter.HandleFunc("/getfiles/{repoName}", s.handleGetRepositoryFiles()).Methods("GET", "OPTIONS")
	repRouter.HandleFunc("/getperms/{repoName}", s.handleGetRepositoryPerms()).Methods("GET", "OPTIONS")
	repRouter.HandleFunc("/addperms/{repoName}", s.handleAddRepositoryPerms()).Methods("POST", "OPTIONS")
	repRouter.HandleFunc("/editperms/{repoName}", s.handleEditRepositoryPerms()).Methods("PATCH", "OPTIONS")
	repRouter.HandleFunc("/removeperms/{repoName}", s.handleRemoveRepositoryPerms()).Methods("DELETE", "OPTIONS")

}

func (s *server) authorizeUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("token")
		if err != nil {
			if err == http.ErrNoCookie {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		tokenStr := cookie.Value

		claims := &Claims{}

		tkn, err := jwt.ParseWithClaims(tokenStr, claims,
			func(t *jwt.Token) (interface{}, error) {
				return jwtKey, nil
			})

		if err != nil {
			if err == jwt.ErrSignatureInvalid {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !tkn.Valid {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), "role", claims.RoleID)
		r = r.WithContext(ctx)

		rw := &responseWriter{w, http.StatusOK}
		next.ServeHTTP(rw, r)
	})
}

func (s *server) handleRemoveRepositoryPerms() http.HandlerFunc {
	type request struct {
		RoleTitle  string `json:"role_title"`
		Path       string `json:"path"`
		Permission string `json:"permission"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusBadRequest, err)
			return
		}
		s.logger.Info("REMOVE PERMS")
		vars := mux.Vars(r)
		repoName := vars["repoName"]

		rp := &model.RepoPerms{
			RoleTitle:  req.RoleTitle,
			Path:       req.Path,
			Permission: req.Permission,
		}

		if err := s.service.RemoveRepositoryPerms(r.Context(), repoName, *rp); err != nil {
			s.error(w, r, http.StatusForbidden, err)
			return
		}

		s.respond(w, r, http.StatusOK, rp)
	}
}

func (s *server) handleEditRepositoryPerms() http.HandlerFunc {
	type request struct {
		Id         int    `json:"id"`
		RoleTitle  string `json:"role_title"`
		Path       string `json:"path"`
		Permission string `json:"permission"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusBadRequest, err)
			return
		}
		s.logger.Info("EDIT PERMS")
		vars := mux.Vars(r)
		repoName := vars["repoName"]

		rp := &model.RepoPermsId{
			Id:         req.Id,
			RoleTitle:  req.RoleTitle,
			Path:       req.Path,
			Permission: req.Permission,
		}

		if err := s.service.EditRepositoryPerms(r.Context(), repoName, *rp); err != nil {
			s.error(w, r, http.StatusForbidden, err)
			return
		}

		s.respond(w, r, http.StatusOK, rp)
	}
}

func (s *server) handleAddRepositoryPerms() http.HandlerFunc {
	type request struct {
		RoleTitle  string `json:"role_title"`
		Path       string `json:"path"`
		Permission string `json:"permission"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusBadRequest, err)
			return
		}
		s.logger.Info("POST NEW PERMS")
		vars := mux.Vars(r)
		repoName := vars["repoName"]

		rp := &model.RepoPerms{
			RoleTitle:  req.RoleTitle,
			Path:       req.Path,
			Permission: req.Permission,
		}

		if err := s.service.AddRepositoryPerms(r.Context(), repoName, *rp); err != nil {
			s.error(w, r, http.StatusForbidden, err)
			return
		}

		s.respond(w, r, http.StatusOK, rp)
	}
}

func (s *server) handleGetRepositoryPerms() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("GET LIST OF FILES OF REPOSITORY")
		vars := mux.Vars(r)
		repoName := vars["repoName"]

		files, err := s.service.GetRepositoryPerms(r.Context(), repoName)
		if err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, files)
	}
}

func (s *server) handleGetRepositoryFiles() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("GET LIST OF FILES OF REPOSITORY")
		vars := mux.Vars(r)
		repoName := vars["repoName"]

		files, err := s.service.GetRepositoryFiles(r.Context(), repoName)
		if err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, files)
	}
}

func (s *server) handleGetRepositories() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("GET LIST OF REPOSITORIES")
		files, err := s.service.GetRepositories(r.Context())
		if err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, files)
	}
}

func (s *server) handleUpload() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("UPLOAD FILE")
		w.Header().Set("Content-Type", "form/json")

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			s.error(w, r, http.StatusBadRequest, err)
			return
		}

		files, ok := r.MultipartForm.File["file"]
		if !ok || len(files) == 0 {
			err := errors.New("file not found")
			s.error(w, r, http.StatusBadRequest, err)
			return
		}

		dir, ok := r.MultipartForm.Value["dir"]
		if !ok {
			err := errors.New("dir not found")
			s.error(w, r, http.StatusBadRequest, err)
			return
		}

		for _, file := range files {
			fileReader, err := file.Open()
			if err != nil {
				s.error(w, r, http.StatusBadRequest, err)
			}

			name := strings.ReplaceAll(file.Filename, " ", "_")
			if err != nil {
				s.error(w, r, http.StatusBadRequest, err)
			}
			name = fmt.Sprintf("%s%s/%s", prefix, dir[0], name)
			fileType := file.Header.Get("Content-Type")

			f := storage.Upload{
				Name: name,
				Type: fileType,
				Size: file.Size,
				Data: fileReader,
			}

			if err := s.service.UploadFile(r.Context(), &f); err != nil {
				s.error(w, r, http.StatusBadRequest, err)
			}
		}

		s.respond(w, r, http.StatusCreated, nil)
	}
}

func (s *server) handleGetFile() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		fileName := strings.Join(strings.Split(r.URL.Path, "/")[2:], "/")
		if !(fileName == "") {
			file, err := s.service.GetFile(r.Context(), prefix+fileName)
			if err != nil {
				s.error(w, r, http.StatusInternalServerError, err)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(int(file.Size)))
			w.Header().Set("Content-Type", file.Type)
			io.Copy(w, file.Obj)
			s.respond(w, r, http.StatusOK, file)
		} else {
			s.logger.Info("Get files from bucket")
			files, err := s.service.GetFiles(r.Context())
			if err != nil {
				s.error(w, r, http.StatusInternalServerError, err)
				return
			}

			s.respond(w, r, http.StatusOK, files)
			return
		}

	}
}

func (s *server) handleRemoveFile() http.HandlerFunc {
	type request struct {
		Filename string `json:"filename"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("DELETE FILE")
		w.Header().Set("Content-type", "application/json")

		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		filename := prefix + req.Filename

		if err := s.service.RemoveFile(r.Context(), filename); err != nil {
			s.error(w, r, http.StatusConflict, err)
			return
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) handleRenameFile() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("RENAME FILE")
		req := &storage.Rename{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
		}
		req.New = prefix + req.New
		req.Old = prefix + req.Old

		if err := s.service.RenameFile(r.Context(), *req); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) handleMoveFile() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("MOVE FILE")
		req := &storage.Move{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}
		req.Dst = prefix + req.Dst
		req.Src = prefix + req.Src

		if err := s.service.MoveFile(r.Context(), *req); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) handleCreateDirectory() http.HandlerFunc {
	type request struct {
		Dir string `json:"dir"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("CREATE DIRECTORY")
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		if err := s.service.CreateDirectory(r.Context(), prefix+req.Dir); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusCreated, nil)
	}
}

func (s *server) handleCreateRepository() http.HandlerFunc {
	type request struct {
		Dir string `json:"dir"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("CREATE REPOSITORY")
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		if err := s.service.CreateRepository(r.Context(), prefix+req.Dir); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusCreated, nil)
	}
}

func (s *server) handleRenameDirectory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("RENAME DIRECTORY")
		req := &storage.Rename{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		req.New = prefix + req.New
		req.Old = prefix + req.Old

		if err := s.service.RenameDirectory(r.Context(), *req); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) handleMoveDirectory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("MOVE DIRECTORY")
		req := &storage.Move{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		req.Dst = prefix + req.Dst
		req.Src = prefix + req.Src

		if err := s.service.MoveDirectory(r.Context(), *req); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) handleRemoveDirectory() http.HandlerFunc {
	type request struct {
		Dir string `json:"dir"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("REMOVE DIRECTORY")
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		if err := s.service.RemoveDirectory(r.Context(), prefix+req.Dir); err != nil {
			s.error(w, r, http.StatusInternalServerError, err)
			return
		}

		s.respond(w, r, http.StatusOK, nil)
	}
}

func (s *server) respond(w http.ResponseWriter, r *http.Request, code int, data interface{}) {
	w.WriteHeader(code)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

func (s *server) error(w http.ResponseWriter, r *http.Request, code int, err error) {
	s.respond(w, r, code, map[string]string{"error": err.Error()})
}
