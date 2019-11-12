# reconEngine
--
It is the storage engine realised the lsm tree structure, used by [ReconDB](https://github.com/Mnwa/Recon)

## Usage

#### Interface MemStorage

```go
//Base mem interface, you can implement own realisation
type MemStorage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte)
	Del(key []byte) error
	Sync() error
	Len() int
	SsTable() SsTableStorage
}
```

```go
// Mem constructor, create structure realised MemStorage interface
// ssTable argument may be a nil
func NewMem(ssTable SsTableStorage) MemStorage
```

#### Interface SsTableStorage

```go
//Base SsTable interface, you can implement own realisation
type SsTableStorage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Del(key []byte) error
	CreatePartition() SsTablePartitionStorage
	ClosePartition(partition SsTablePartitionStorage) error
	OpenPartition(createdAt int64) SsTablePartitionStorage
	Range(cb func(createdAt int64, partitionStorage SsTablePartitionStorage) bool)
	Len() int
	CloseAll() error
	MergeSort() error
}
```

```go
// SsTable constructor, create structure realised SsTableStorage interface
func NewSsTable() SsTableStorage
```

#### Interface SsTablePartitionStorage

```go
//Base ss table partition interface, you can implement own realisation
type SsTablePartitionStorage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Del(key []byte) error
	Range(cb func(key []byte, value []byte) bool)
	Key() int64
	Close() error
}
```

```go
// SsTable partition constructor, create structure realised SsTablePartitionStorage interface
func NewSStablePartition(createdAt int64) SsTablePartitionStorage
```

#### Directory to write saved data
```go
// partitions will be written here
var BinDir = "bin"
```

#### Errors
```go
// Error used when key don't exists
var KeyNotFoundErr = errors.New("can't found value by that key")
```
```go
// Error used when key removed
var KeyRemovedErr = errors.New("that key was removed")
```