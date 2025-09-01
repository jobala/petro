# petro

A simple key value storage engine

## Getting Started

### Installation

`go get github.com/jobala/petro`

### Put

```go
store, err := index.New[string, int]("index", dbFile)
ok, err := store.Put("age", 25)
```

### Get

```go
store := index.New[string, int]("index", dbFile)
val, err := store.Get("age")
```

### PutBatch

```go
ages := map[string]int{
    "john": 30,
    "jane": 20,
    "doe": 45
}

store := index.New[string, int]("index", dbFile)
err := store.PutBatch(ages)
```

### GetKeyRange

```go
ages := map[string]int{
    "john": 30,
    "jane": 20,
    "doe": 45
}

store := index.New[string, int]("index", dbFile)
store.GetKeyRange("doe", "jane")
```

### Iterate

```go
ages := map[string]int{
    "john": 30,
    "jane": 20,
    "doe": 45
}

store := index.New[string, int]("index", dbFile)
storeIter := store.GetIterator()

for !storeIter.IsEnd() {
    key, val, err := storeIter.Next()
}
```

### durability

call `store.flush()` to ensure that your data is written to disk

## Design Notes

- [Disk Management](https://japhethobala.com/posts/technical/db-disk-mgmt)
- [Bufferpool Management](https://japhethobala.com/posts/technical/db-buffer-mgmt/)

## Roadmap

- [x] Disk Management
- [x] Bufferpool Management
- [x] Index Management
  - [x] GetValue
  - [x] Insert
  - [x] Iterator
  - [ ] Delete
  - [ ] Support duplicate keys
- [ ] Transactions
- [ ] Recovery
