package upload

import (
    "api_mgr/configs"
    "api_mgr/utils"
    "archive/zip"
    "errors"
    "fmt"
    "io"
    "io/fs"
    "io/ioutil"
    "mime/multipart"
    "net/url"
    "os"
    "path/filepath"
    "strings"
    "time"

    "git.yj.live/Golang/source/configmanager"
    "git.yj.live/Golang/source/log"
    "github.com/google/uuid"
)

// ResourceType 资源类型
type ResourceType int32

// const .
const (
    RT_UNKNOWN              ResourceType = 0
    RT_GAME_ICON            ResourceType = 1  // 1 游戏icon
    RT_GAMEITEM_ICON        ResourceType = 2  // 2 游戏详情icon
    RT_LOADING              ResourceType = 3  // 3 loading
    RT_ACTIVITY_EVENT       ResourceType = 4  // 4 活动事件管理
    RT_DOCUMENTS_AGENCY     ResourceType = 5  // 5 文档素材
    RT_OFFICE_ICON          ResourceType = 6  // 6 官网ICON
    RT_RECOMMEND_ICON       ResourceType = 7  // 7 推荐列表ICOn
    RT_AGENT_CONTROL        ResourceType = 8  // 8 代理人调控
    RT_CURRENCY             ResourceType = 9  // 9 货币
    RT_GAME_BRAND           ResourceType = 10 // 游戏品牌
    RT_MULTIPART            ResourceType = 11 // 分片类型
    RT_GAME_HALL            ResourceType = 12 // 游戏大厅
    RT_GAME_SKIN            ResourceType = 13 // 游戏皮肤
    RT_GMAE_SHARE           ResourceType = 14 // 游戏分享, 此处仅占位作用，不能上传
    RT_GAME_BRAND_HALL_ICON ResourceType = 15 // 游戏大厅的icon
    // DIR_FILE_MODE 目录权限
    DIR_FILE_MODE fs.FileMode = 0755
)

// Enum value maps for resourceType
var (
    ResourceTypeName = map[ResourceType]string{
        RT_UNKNOWN:              "UNKNOWN",
        RT_GAME_ICON:            "/icon",
        RT_GAMEITEM_ICON:        "/gameitemicon",
        RT_LOADING:              "/loading",
        RT_ACTIVITY_EVENT:       "/activity",
        RT_DOCUMENTS_AGENCY:     "/documents",
        RT_OFFICE_ICON:          "/office_icon",
        RT_RECOMMEND_ICON:       "/recommendicon",
        RT_AGENT_CONTROL:        "/agent_control",
        RT_CURRENCY:             "/currency",
        RT_GAME_BRAND:           "/gamebrand",
        RT_MULTIPART:            "/tmp",
        RT_GAME_HALL:            "/game_hall",
        RT_GAME_SKIN:            "/game_skin",
        RT_GMAE_SHARE:           "/game_share",
        RT_GAME_BRAND_HALL_ICON: "/brand_hall_icon",
    }
)

// Storage .
type Storage struct {
    // CDN路径
    cdnPath string
    // 上传路径, 调用文件上传接口，上传文件到上传路径
    // 然后将文件copy到CDN路径
    uploadPath string
    version    string
    // 文件存放位置
    where             string
    resourceType      ResourceType
    resourceId        string
    customeResourceId bool
    delayJob          *StorageDelayJob
}

// NewStorage .
func NewStorage(resourceType ResourceType, resourceId string) (*Storage, error) {
    if _, ok := ResourceTypeName[resourceType]; !ok || resourceType == RT_UNKNOWN {
        return nil, fmt.Errorf("invalid resource_type '%d'", resourceType)
    }
    storage := &Storage{
        uploadPath:        configs.Config.Upload.UploadPath, // 图片上传的目录
        cdnPath:           configs.Config.Upload.RootPath,   // 这个是cdn目录（图片上传成功复制到cdn目录：UploadPath =》RootPath）
        resourceType:      resourceType,
        resourceId:        resourceId,
        version:           fmt.Sprintf("%d", time.Now().Unix()),
        customeResourceId: resourceId != "",
        delayJob:          NewStorageDelayJob(),
    }
    if resourceId == "" {
        storage.resourceId = uuid.NewString() + "_" + configmanager.GetString("hostname", "1")
    }
    if resourceType == RT_DOCUMENTS_AGENCY || resourceType == RT_AGENT_CONTROL {
        // 这两种上传目录特殊处理
        storage.cdnPath = configs.Config.Upload.DownloadPath
    }
    return storage, nil
}

// Upload 调用此方法需要调用UploadAndRename方法删除延迟任务，不然一段时间后文件将会被删除
// 上传文件到上传路径
func (s *Storage) Upload(file *multipart.FileHeader) (string, error) {
    if err := s.uploadValid(file); err != nil {
        return "", err
    }
    uploadPath := s.uploadFullPathByName(file.Filename)
    dstFile, err := os.Create(uploadPath)
    if err != nil {
        log.L().Errorf("create file '%s' fail[%s]", uploadPath, err.Error())
        return "", err
    }
    defer dstFile.Close()
    uploadFile, err := file.Open()
    if err != nil {
        log.L().Errorf("open file '%s' fail[%s]", file.Filename, err.Error())
        return "", err
    }
    defer uploadFile.Close()
    if _, err := io.Copy(dstFile, uploadFile); err != nil {
        log.L().Errorf("save upload file '%s' to '%s' fail[%s]", file.Filename, uploadPath, err.Error())
        return "", err
    }
    // 添加延迟任务删除临时文件
    s.delayJob.Add(uploadPath)
    return fmt.Sprintf("%s?v=%s&where=upload", s.fileName(file.Filename), s.version), nil
}

// UploadAndRename 文件上传
// 将文件从uploadpath拷贝到CDNpath
func (s *Storage) UploadAndRename(uploadPath string) (string, error) {
    uploadFullPath, err := s.UploadFullPathByPath(uploadPath)
    if err != nil {
        return "", err
    }
    if uploadFullPath != "" {
        // 拷贝到自定义目录
        if err := s.copy(uploadFullPath); err != nil {
            return "", err
        }
        return fmt.Sprintf("%s?v=%s", s.fileName(uploadFullPath), s.version), nil
    }
    return uploadPath, nil
}

// UnzipAndDelete 解压到指定目录并删除
func (s *Storage) UnzipAndDelete(uploadPath string) error {
    return s.UnzipAndDeleteWithPath(uploadPath, "")
}

// UnzipAndDeleteWithPath 解压到指定目录并删除
func (s *Storage) UnzipAndDeleteWithPath(uploadPath string, unzipPath string) error {
    uploadPath = s.parse(uploadPath)
    unzipDir := filepath.Join(s.cdnPath, ResourceTypeName[s.resourceType])
    if unzipPath != "" {
        unzipDir = filepath.Join(unzipDir, unzipPath)
    }
    if !utils.IsExist(unzipDir) {
        if err := os.MkdirAll(unzipDir, 0755); err != nil {
            return fmt.Errorf("create dir '%s' fail[%s]", unzipDir, err.Error())
        }
    }
    uploadPath = filepath.Join(s.uploadPath, uploadPath)
    if !utils.IsExist(uploadPath) {
        return fmt.Errorf("file '%s' not found", uploadPath)
    }

    reader, err := zip.OpenReader(uploadPath)
    if err != nil {
        log.L().Errorf("unzip file '%s' fail[%s]", uploadPath, err.Error())
        return err
    }
    defer reader.Close()
    for _, item := range reader.File {
        filePath := filepath.Join(unzipDir, item.Name)
        if item.FileInfo().IsDir() {
            if err := os.MkdirAll(filePath, 0755); err != nil {
                log.L().Errorf("create dir '%s' fail[%s]", filePath, err.Error())
                return err
            }
            continue
        } else {
            if dir := filepath.Dir(filePath); !utils.IsExist(dir) {
                if err := os.MkdirAll(dir, 0755); err != nil {
                    log.L().Errorf("create dir '%s' fail[%s]", dir, err.Error())
                    return err
                }
            }
        }
        rc, err := item.Open()
        if err != nil {
            return fmt.Errorf("open file '%s' fail[%s]", item.Name, err.Error())
        }
        dst, err := os.Create(filePath)
        if err != nil {
            log.L().Errorf("create file '%s' fail[%s]", filePath, err.Error())
            return err
        }
        if _, err := io.Copy(dst, rc); err != nil {
            log.L().Errorf("write file '%s' into '%s' fail[%s]", item.Name, filePath, err.Error())
            return err
        }
        dst.Close()
        rc.Close()
    }
    if err := os.Remove(uploadPath); err != nil {
        log.L().Errorf("remove file '%s' fail[%s]", uploadPath, err.Error())
        return err
    }
    return nil
}

// UploadFullPathByPath 上传全路径
func (s *Storage) UploadFullPathByPath(uploadPath string) (string, error) {
    uploadPath = s.parse(uploadPath)
    if s.where == "" {
        return "", nil
    }
    uploadPath = filepath.Join(s.uploadPath, uploadPath)
    if !utils.IsExist(uploadPath) {
        return "", fmt.Errorf("file '%s' not found", uploadPath)
    }
    return uploadPath, nil
}

// UploadFullPath 上传文件全路径
func (s *Storage) uploadFullPathByName(fileName string) string {
    filePath := filepath.Join(s.uploadPath, s.fileName(fileName))
    fileDir := filepath.Dir(filePath)
    if !utils.IsExist(fileDir) {
        if err := os.MkdirAll(fileDir, 0755); err != nil {
            log.L().Errorf("mkdir '%s' fail[%s]", fileDir, err.Error())
            return filePath
        }
    }
    return filePath
}

// cdn文件路径
func (s *Storage) cdnFullPath(uploadFullPath string) string {
    cdnFullPath := filepath.Join(s.cdnPath, s.fileName(uploadFullPath))
    fileDir := filepath.Dir(cdnFullPath)
    if !utils.IsExist(fileDir) {
        if err := os.MkdirAll(fileDir, 0755); err != nil {
            log.L().Errorf("mkdir '%s' fail[%s]", fileDir, err.Error())
            return cdnFullPath
        }
    }
    return cdnFullPath
}

// FileName /resourceType/resourceId.suffix
func (s *Storage) fileName(fileName string) string {
    return filepath.Join(ResourceTypeName[s.resourceType],
        fmt.Sprintf("%s%s", s.resourceId, filepath.Ext(fileName)))
}

func (s *Storage) parse(uploadPath string) string {
    u, err := url.Parse(uploadPath)
    if err == nil {
        uploadPath = u.Path
        tempVersion := u.Query().Get("v")
        if tempVersion != "" {
            s.version = tempVersion
        }
        s.where = u.Query().Get("where")
    } else {
        log.L().Errorf("parse upload_path fail[%s]", err.Error())
    }
    return uploadPath
}

// 上传校验
func (s *Storage) uploadValid(file *multipart.FileHeader) error {
    if err := s.uploadSizeValid(file); err != nil {
        return err
    }
    if err := s.uploadSuffixValid(file); err != nil {
        return err
    }
    return nil
}

func (s *Storage) uploadSuffixValid(file *multipart.FileHeader) error {
    suffixes := s.uploadSuffixLimit()
    if len(suffixes) != 0 {
        fileSuffix := filepath.Ext(file.Filename)
        exist := false
        for _, suffix := range suffixes {
            if strings.TrimSpace(suffix) == fileSuffix {
                exist = true
                break
            }
        }
        if !exist {
            return fmt.Errorf("unsupport file suffix '%s' on resource_type '%d', support %v", fileSuffix, s.resourceType, suffixes)
        }
    }
    return nil
}

// 上传大小校验
func (s *Storage) uploadSizeValid(file *multipart.FileHeader) error {
    sizeLimit := s.uploadSizeLimit()
    if file.Size > sizeLimit {
        return fmt.Errorf("upload size exceed limit(%d, %d)", file.Size, sizeLimit)
    }
    return nil
}

// 支持的文件后缀， 多个后缀以英文逗号分隔
func (s *Storage) uploadSuffixLimit() []string {
    return strings.Split(configmanager.GetString(fmt.Sprintf("upload.%d.accept_suffixes", s.resourceType),
        configmanager.GetString("upload.accept_suffixes", ".jpg,.jpeg,.png,.zip,.csv,.json,.atlas,.xls,.xlsx")), ",")
}

func (s *Storage) uploadSizeLimit() int64 {
    return configmanager.GetInt64(fmt.Sprintf("upload.%d.max_size", s.resourceType),
        configmanager.GetInt64("upload.max_size", 1024*1024*5))
}

// upload/ -> cdn/
// copy 如果是本机重命名, 如果不是需要下载到指定文件
func (s *Storage) copy(uploadFullPath string) error {
    // 保存目录更换为cdn目录
    cdnFullPath := s.cdnFullPath(uploadFullPath)
    log.L().Debugf("move file '%s' to '%s' need rename '%v'", uploadFullPath, cdnFullPath, s.customeResourceId)
    if s.customeResourceId && cdnFullPath != uploadFullPath {
        log.L().Debugf("move file--22 '%s' to '%s' need rename '%v'", uploadFullPath, cdnFullPath, s.customeResourceId)
        if utils.IsExist(uploadFullPath) {
            if err := copy(uploadFullPath, cdnFullPath); err != nil {
                log.L().Errorf("copy file '%s' to '%s' fail[%s]", uploadFullPath, cdnFullPath, err.Error())
                return err
            }
        } else {
            // 下载到指定文件
            log.L().Warnf("file '%s' not found in current host", uploadFullPath)
        }
    } else {
        log.L().Debugf("move file--33 '%s' to '%s' need rename '%v'", uploadFullPath, cdnFullPath, s.customeResourceId)
        // 从延迟队列删除
        if s.customeResourceId == false {
            log.L().Debugf("move file--44 '%s' to '%s' need rename '%v'", uploadFullPath, cdnFullPath, s.customeResourceId)
            s.delayJob.Remove(uploadFullPath)
        } else {
            log.L().Debugf("move file--55 '%s' to '%s' need rename '%v'", uploadFullPath, cdnFullPath, s.customeResourceId)
            s.delayJob.Remove(uploadFullPath)
        }
    }
    return nil
}

// 拷贝文件
func copyDir(src, dest string) error {
    if !isExist(dest) {
        if err := os.MkdirAll(dest, DIR_FILE_MODE); err != nil {
            return err
        }
    }
    dirs, err := ioutil.ReadDir(src)
    if err != nil {
        return err
    }
    for _, fs := range dirs {
        srcPath := filepath.Join(src, fs.Name())
        destPath := filepath.Join(dest, fs.Name())
        if fs.IsDir() {
            copyDir(srcPath, destPath)
        }
        copyFile(srcPath, destPath)
    }
    return nil
}

// 拷贝文件
func copyFile(src, dest string) error {
    destDirPath := filepath.Dir(dest)
    if !isExist(destDirPath) {
        if err := os.MkdirAll(destDirPath, DIR_FILE_MODE); err != nil {
            return err
        }
    }
    sfile, err := os.Open(src)
    if err != nil {
        return err
    }
    defer sfile.Close()
    dfile, err := os.OpenFile(dest, os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.ModePerm)
    if err != nil {
        return err
    }
    defer dfile.Close()
    if _, err := io.Copy(dfile, sfile); err != nil {
        return err
    }
    return nil
}

// 拷贝文件或目录
func copy(src, dest string) error {
    if !isExist(src) {
        return fmt.Errorf("path '%s' not found", src)
    }
    if err := isDir(src); err == nil {
        // 如果源文件是个目录,则目标文件必须是个目录
        return copyDir(src, dest)
    }
    return copyFile(src, dest)
}

func isDir(file string) error {
    s, err := os.Stat(file)
    if err != nil {
        return fmt.Errorf("path '%s' not exist", file)
    }
    if !s.IsDir() {
        return fmt.Errorf("path '%s' not a dir", file)
    }
    return nil
}

// isExist 文件或目录是否存在
func isExist(file string) bool {
    _, err := os.Stat(file)
    if err != nil {
        return false
    }
    return true
}

// CdnFilePath 返回不同业务cdn存放的目录
func CdnFilePath(resourceType ResourceType) (string, error) {
    if _, ok := ResourceTypeName[resourceType]; ok {
        return filepath.Join(configs.Config.Upload.RootPath, ResourceTypeName[resourceType]), nil
    }
    return "", errors.New("resourceType no exists")
}
