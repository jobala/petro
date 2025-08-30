package util

import (
	"github.com/jobala/petro/storage/disk"
	"github.com/vmihailenco/msgpack"
)

func ToByteSlice[T any](obj T) ([]byte, error) {
	res := make([]byte, disk.PAGE_SIZE)

	data, err := msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}
	copy(res, data)

	return res, nil
}

func ToStruct[T any](data []byte) (T, error) {
	var res T

	if err := msgpack.Unmarshal(data, &res); err != nil {
		return res, nil
	}

	return res, nil
}
