package upload

import (
	"api_mgr/configs"
	merr "api_mgr/model/errors"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"os"
	"path/filepath"
	pb "protos_repo/file"
	"sort"

	errDef "git.yj.live/Golang/source/errors"

	"git.yj.live/Golang/source/configmanager"
	"git.yj.live/Golang/source/log"
	"google.golang.org/grpc/codes"
)

const (
	// MULTIPART_STORAGE_METADATA 分片上传存储元数据
	MULTIPART_STORAGE_METADATA = "platform:multipart_storage:%s:metadata"
	// MULTIPART_STORAGE_CHUNKS 分片上传存储分块数据
	MULTIPART_STORAGE_CHUNKS = "platform:multipart_storage:%s:chunks"
	// MULTIPART_STORAGE_CHUNKS_HASH 分片上传存储分块数据，升级为保存到hash
	MULTIPART_STORAGE_CHUNKS_HASH = "platform:multipart_storage:hash:%s:chunks"
)

// MultipartStorage 分片上传存储
type MultipartStorage struct {
	*Storage
	// 如果不为空需要校验文件完整性
	contentMD5 string
	// 文件大小, 如果不为空需要校验文件大小
	size int64
}

// NewMultipartStorage 分片上传
func NewMultipartStorage(resourceType ResourceType, resourceId string) (*MultipartStorage, error) {
	storage, err := NewStorage(resourceType, resourceId)
	if err != nil {
		log.L().Errorf("new storage fail[%s]", err.Error())
		return &MultipartStorage{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	return &MultipartStorage{Storage: storage}, nil
}

// Start 分片上传准备
// 存储在redis中
func (s *MultipartStorage) Start(in *pb.MultipartUploadStartReq) (*pb.MultipartUploadStartInfo, error) {
	if err := s.setStart(in); err != nil {
		return &pb.MultipartUploadStartInfo{}, err
	}
	return &pb.MultipartUploadStartInfo{
		UploadId: s.resourceId,
	}, nil
}

// Upload 分片上传
func (s *MultipartStorage) Upload(in *pb.MultipartUploadReq, file *multipart.FileHeader) (*pb.MultipartUploadChunkInfo, error) {
	_, err := s.getStartInfo(in.UploadId)
	if err != nil {
		return &pb.MultipartUploadChunkInfo{}, err
	}
	s.contentMD5 = in.ContentMd5
	s.size = in.Size
	// 上传文件
	downloadPath, err := s.upload(file)
	if err != nil {
		return &pb.MultipartUploadChunkInfo{}, err
	}
	chunkInfo := &pb.MultipartUploadChunkInfo{
		UploadId:     in.UploadId,
		Chunk:        in.Chunk,
		ContentMd5:   in.ContentMd5,
		Validity:     s.delayJob.delayDuration().String(),
		DownloadPath: downloadPath,
	}
	if err := s.setChunk(chunkInfo); err != nil {
		return &pb.MultipartUploadChunkInfo{}, err
	}
	return chunkInfo, nil
}

// Done 分片文件上传结束
func (s *MultipartStorage) Done(in *pb.MultipartUploadIDReq) (*pb.MultipartUploadDoneResp, error) {
	startInfo, err := s.getStartInfo(in.UploadId)
	if err != nil {
		return &pb.MultipartUploadDoneResp{}, err
	}
	s.resourceType = ResourceType(startInfo.Type)
	// 获取所有分片
	chunks, err := s.getChunks(in.UploadId)
	if err != nil {
		return &pb.MultipartUploadDoneResp{}, err
	}
	if len(chunks) != int(startInfo.Chunks) {
		return &pb.MultipartUploadDoneResp{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INVALID_REQUEST_ERR,
			codes.InvalidArgument,
			"upload '%s' chunks not enough, current %d, want %d", in.UploadId, len(chunks), startInfo.Chunks)
	}
	// 创建目标文件
	filePath := s.uploadFullPathByName(startInfo.Filename)
	dstFile, err := os.Create(filePath)
	if err != nil {
		log.L().Errorf("multipart upload create file '%s' fail[%s]", filePath, err.Error())
		return &pb.MultipartUploadDoneResp{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	defer dstFile.Close()
	hash := md5.New()
	// 合并文件
	for _, chunk := range chunks {
		chunkFile, err := ioutil.ReadFile(filepath.Join(s.uploadPath, s.parse(chunk.DownloadPath)))
		if err != nil {
			log.L().Errorf("open chunk file '%s' fail[%s]", chunk.DownloadPath, err.Error())
			return &pb.MultipartUploadDoneResp{}, errDef.Errorf(merr.SYSTEM_CODE,
				errDef.INTERNAL_SERVER_ERR,
				codes.Internal,
				"internal server error")
		}
		if _, err := dstFile.Write(chunkFile); err != nil {
			log.L().Errorf("merge chunk '%s' fail[%s]", chunk.DownloadPath, err.Error())
			return &pb.MultipartUploadDoneResp{}, errDef.Errorf(merr.SYSTEM_CODE,
				errDef.INTERNAL_SERVER_ERR,
				codes.Internal,
				"internal server error")
		}
		if _, err := hash.Write([]byte(chunk.ContentMd5)); err != nil {
			log.L().Errorf("write chunkfile into md5 hash fail[%s]", err.Error())
			return &pb.MultipartUploadDoneResp{}, errDef.Errorf(merr.SYSTEM_CODE,
				errDef.INTERNAL_SERVER_ERR,
				codes.Internal,
				"internal server error")
		}
	}
	// 文件完整性校验
	if err := s.contentMD5Valid(hex.EncodeToString(hash.Sum(nil))); err != nil {
		return &pb.MultipartUploadDoneResp{}, err
	}

	// 添加延迟任务删除临时文件
	s.delayJob.Add(filePath)
	return &pb.MultipartUploadDoneResp{
		UploadId:     in.UploadId,
		DownloadPath: fmt.Sprintf("%s?v=%s&where=multi_upload", s.fileName(filePath), s.version),
		Validity:     s.delayJob.delayDuration().String(),
	}, nil
}

// GetMultipartUploadChunk 获取分块详情
func (s *MultipartStorage) GetMultipartUploadChunk(in *pb.MultipartUploadChunkReq) (*pb.MultipartUploadChunkInfo, error) {
	// chunkInfo, err := configs.RedisCli.Get(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS, fmt.Sprintf("%s_%d", in.UploadId, in.Chunk))).Bytes()
	chunkInfo, err := configs.RedisCli.HGet(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId), fmt.Sprintf("%d", in.Chunk)).Bytes()
	if err != nil || len(chunkInfo) == 0 {
		return &pb.MultipartUploadChunkInfo{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INVALID_REQUEST_ERR,
			codes.InvalidArgument,
			"upload '%s' chunk %d not found", in.UploadId, in.Chunk)
	}
	var multipartUploadChunkInfo pb.MultipartUploadChunkInfo
	if err := json.Unmarshal(chunkInfo, &multipartUploadChunkInfo); err != nil {
		log.L().Errorf("unmarshal upload '%s' chunk %d fail[%s]", in.UploadId, in.Chunk, string(chunkInfo))
		return &pb.MultipartUploadChunkInfo{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	return &multipartUploadChunkInfo, nil
}

// GetMultipartUploadChunks 获取上传的分块
func (s *MultipartStorage) GetMultipartUploadChunks(in *pb.MultipartUploadIDReq) (*pb.MultipartUploadChunks, error) {
	chunkInfos, err := s.getChunks(in.UploadId)
	if err != nil {
		return &pb.MultipartUploadChunks{}, err
	}
	return &pb.MultipartUploadChunks{Data: chunkInfos}, nil
}

func (s *MultipartStorage) setStart(in *pb.MultipartUploadStartReq) error {
	startInfo, err := json.Marshal(in)
	if err != nil {
		log.L().Errorf("marshal multipart upload repare req fail[%s]", err.Error())
		return errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	// 元数据存储
	if err := configs.RedisCli.Set(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_METADATA, s.resourceId),
		string(startInfo), s.delayJob.delayDuration()).Err(); err != nil {
		log.L().Errorf("redis set multipart upload metadata fail[%s]", err.Error())
		return errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	log.L().Infof("MultipartUpload setStart set redis:%s, expireTime:%v, err:%v", string(startInfo), s.delayJob.delayDuration(), err)
	return nil
}

func (s *MultipartStorage) getStartInfo(uploadId string) (*pb.MultipartUploadStartReq, error) {
	startInfo, err := configs.RedisCli.Get(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_METADATA, uploadId)).Bytes()
	log.L().Infof("MultipartUpload getStartInfo uploadId:%s, startInfo:%v,err:%v", uploadId, string(startInfo), err)
	if err != nil || len(startInfo) == 0 {
		return &pb.MultipartUploadStartReq{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INVALID_REQUEST_ERR,
			codes.InvalidArgument,
			"upload '%s' not found", uploadId)
	}
	var multiPartUploadStartInfo pb.MultipartUploadStartReq
	if err := json.Unmarshal(startInfo, &multiPartUploadStartInfo); err != nil {
		log.L().Errorf("multipart upload unmarshal repare request '%s' fail[%s]", string(startInfo), err.Error())
		return &pb.MultipartUploadStartReq{}, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	log.L().Infof("MultipartUpload getStartInfo uploadId:%s, multiPartUploadStartInfo:%+v,err:%v", uploadId, multiPartUploadStartInfo, err)
	return &multiPartUploadStartInfo, nil
}

func (s *MultipartStorage) setChunk(in *pb.MultipartUploadChunkInfo) error {
	chunkInfo, err := json.Marshal(in)
	if err != nil {
		log.L().Errorf("multipart upload marshal upload fail[%s]", err.Error())
		return errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}

	// redis保存
	// if err := configs.RedisCli.Set(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS, fmt.Sprintf("%s_%d", in.UploadId, in.Chunk)), string(chunkInfo), s.delayJob.delayDuration()).Err(); err != nil {
	// 	log.L().Errorf("multipart upload set chunk info into redis fail[%s]", err.Error())
	// 	return errDef.Errorf(merr.SYSTEM_CODE,
	// 		errDef.INTERNAL_SERVER_ERR,
	// 		codes.Internal,
	// 		"internal server error")
	// }

	// redis-hash 保存
	if err := configs.RedisCli.HSet(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId),
		fmt.Sprintf("%d", in.Chunk), string(chunkInfo)).Err(); err != nil {
		return errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}

	log.L().Infof("MultipartUpload setChunk chunkInfo:%v,expireTime:%v,redis_hash_key:%s,key:%d,err:%v",
		in, s.delayJob.delayDuration(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId), in.Chunk, err)

	ttl, _ := configs.RedisCli.TTL(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId)).Result()
	if ttl == -1 {
		err = configs.RedisCli.Expire(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId), s.delayJob.delayDuration()).Err()
		if err != nil {
			log.L().Errorf("multipart upload set chunk redis hash:%s expire error:%s",
				fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, in.UploadId), s.delayJob.delayDuration())
		}
	}

	return nil
}
func (s *MultipartStorage) getChunks(uploadId string) ([]*pb.MultipartUploadChunkInfo, error) {
	var chunkInfos []*pb.MultipartUploadChunkInfo
	// 使用redis keys 扫描表导致超时，导致上传失败
	// chunkKeys, err := configs.RedisCli.Keys(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS, fmt.Sprintf("%s_*", uploadId))).Result()
	// log.L().Errorf("MultipartUpload getChunks uploadId:%s,redis_key:%s,chunkKeys:%+v, err:%v, end_time:%s ",
	// 	uploadId, fmt.Sprintf(MULTIPART_STORAGE_CHUNKS, fmt.Sprintf("%s_*", uploadId)), chunkKeys, err, time.Now().Format("2006-01-02 15:04:05"))
	// if err != nil || len(chunkKeys) == 0 {
	// 	return chunkInfos, errDef.Errorf(merr.SYSTEM_CODE,
	// 		errDef.INVALID_REQUEST_ERR,
	// 		codes.InvalidArgument,
	// 		"upload '%s' chunks not found", uploadId)
	// }
	//

	// 获取分批的集合信息 redis hash
	chunkInfoList, err := configs.RedisCli.HGetAll(context.Background(), fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, uploadId)).Result()
	log.L().Infof("MultipartUpload getChunks uploadId:%s,redis_key:%s,result:%+v, err:%v",
		uploadId, fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, uploadId), chunkInfoList, err)

	if err != nil || len(chunkInfoList) == 0 {
		return chunkInfos, errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INVALID_REQUEST_ERR,
			codes.InvalidArgument,
			"upload '%s' chunks redis hash not found", uploadId)
	}
	for k := range chunkInfoList {
		if chunkInfoList[k] == "" {
			log.L().Errorf("MultipartStorage getChunks empty.redis hash:%s, key:%s",
				fmt.Sprintf(MULTIPART_STORAGE_CHUNKS_HASH, uploadId), k)
			continue
		}
		var chunkInfo pb.MultipartUploadChunkInfo
		if err := json.Unmarshal([]byte(chunkInfoList[k]), &chunkInfo); err != nil {
			log.L().Errorf("unmarshal chunk info '%s' fail[%s]", string(chunkInfoList[k]), err.Error())
			return chunkInfos, errDef.Errorf(merr.SYSTEM_CODE,
				errDef.INTERNAL_SERVER_ERR,
				codes.Internal,
				"internal server error")
		}
		chunkInfos = append(chunkInfos, &chunkInfo)
	}

	sort.SliceStable(chunkInfos, func(i, j int) bool {
		return chunkInfos[i].Chunk < chunkInfos[j].Chunk
	})
	return chunkInfos, nil
}

// 文件完整性校验
func (s *MultipartStorage) contentValid(file multipart.File) error {
	if s.contentMD5 != "" && configmanager.GetBool("multipart_upload.check.content.enabled", false) {
		hash := md5.New()
		if _, err := io.Copy(hash, file); err != nil {
			log.L().Errorf("content valid check md5sum fail[%s]", err.Error())
			return errDef.Errorf(merr.SYSTEM_CODE,
				errDef.INTERNAL_SERVER_ERR,
				codes.Internal,
				"internal server error")
		}
		md5sum := hex.EncodeToString(hash.Sum(nil))
		if err := s.contentMD5Valid(md5sum); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultipartStorage) contentMD5Valid(contentMD5 string) error {
	if s.contentMD5 != "" && configmanager.GetBool("multipart_upload.check.content.enabled", false) {
		if s.contentMD5 != contentMD5 {
			return errDef.Warnf(merr.SYSTEM_CODE,
				errDef.INVALID_REQUEST_ERR,
				codes.InvalidArgument,
				"upload file content_md5 not equal input '%s', want '%s'", s.contentMD5, contentMD5)
		}
	}
	return nil
}

// 文件大小校验
func (s *MultipartStorage) sizeValid(file *multipart.FileHeader) error {
	if s.size != 0 && configmanager.GetBool("multipart_upload.check.size.enabled", false) {
		maxSizePerChunk := configmanager.GetInt64("multipart_upload.check.size.max", 10<<20)
		if file.Size > maxSizePerChunk {
			return errDef.Warnf(merr.SYSTEM_CODE,
				errDef.INVALID_REQUEST_ERR,
				codes.InvalidArgument,
				"request file too large, max size %d bytes", maxSizePerChunk)
		}
		if err := s.fileSizeValid(file.Size); err != nil {
			return err
		}
	}
	return nil
}

func (s *MultipartStorage) fileSizeValid(size int64) error {
	if s.size != 0 && configmanager.GetBool("multipart_upload.check.size.enabled", false) {
		if s.size != size {
			return errDef.Warnf(merr.SYSTEM_CODE,
				errDef.INVALID_REQUEST_ERR,
				codes.InvalidArgument,
				"upload file size not equal input %d, want %d", s.size, size)
		}
	}
	return nil
}

// 分片上传
func (s *MultipartStorage) upload(file *multipart.FileHeader) (string, error) {
	if err := s.sizeValid(file); err != nil {
		return "", err
	}
	filePath := s.uploadFullPathByName(file.Filename)
	dstFile, err := os.Create(filePath)
	if err != nil {
		log.L().Errorf("multipart upload create file '%s' fail[%s]", filePath, err.Error())
		return "", errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	defer dstFile.Close()
	uploadFile, err := file.Open()
	if err != nil {
		log.L().Errorf("multipart upload open file '%s' fail[%s]", file.Filename, err.Error())
		return "", errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	defer uploadFile.Close()
	if err := s.contentValid(uploadFile); err != nil {
		return "", err
	}
	log.L().Debugf("save resource '%s' to '%s'", s.resourceId, filePath)
	if _, err := io.Copy(dstFile, uploadFile); err != nil {
		log.L().Errorf("save upload file '%s' to '%s' fail[%s]", file.Filename, filePath, err.Error())
		return "", errDef.Errorf(merr.SYSTEM_CODE,
			errDef.INTERNAL_SERVER_ERR,
			codes.Internal,
			"internal server error")
	}
	// 添加延迟任务删除临时文件
	s.delayJob.Add(filePath)
	return fmt.Sprintf("%s?v=%s&where=multi_upload", s.fileName(filePath), s.version), nil
}
