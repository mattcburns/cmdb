package db

import (
	bolt "go.etcd.io/bbolt"
)

var RESERVED_BUCKET_NAMES = []string{
	"cis", "citypes", "cisbytype", "reltypes", "reltypeindex",
	"relsincoming", "relsoutgoing",
}

type DB struct {
	*bolt.DB
}

func NewDB(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("cis"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("cisbytype"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("citypes"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("reltypes"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("reltypeindex"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("relsincoming"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("relsoutgoing"))
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func guardName(name string) error {
	for _, reserved := range RESERVED_BUCKET_NAMES {
		if name == reserved {
			return ErrReservedBucketName
		}
	}

	return nil
}
