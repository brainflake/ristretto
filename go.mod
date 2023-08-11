module github.com/brainflake/ristretto

go 1.12

replace (
	github.com/dgraph-io/ristretto => github.com/brainflake/ristretto v0.1.0
	github.com/dgraph-io/ristretto/z => github.com/brainflake/ristretto/z v0.1.0
)

require (
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/dgraph-io/ristretto v0.0.0-00010101000000-000000000000
	// github.com/dgraph-io/ristretto v0.0.0-00010101000000-000000000000
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/dustin/go-humanize v1.0.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
	github.com/vmihailenco/msgpack/v5 v5.3.5
	golang.org/x/sys v0.0.0-20221010170243-090e33056c14
)
