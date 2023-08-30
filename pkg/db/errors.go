package db

import "errors"

var ErrKeyNotFound = errors.New("key not found")
var ErrNameAlreadyExists = errors.New("name already exists")
var ErrReservedBucketName = errors.New("bucket name is reserved")
