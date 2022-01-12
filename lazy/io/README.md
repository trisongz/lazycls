
# lazy.io

- `cachez` is a fork from [diskcache](https://github.com/grantjenks/python-diskcache/) with some modifications, including: 
    - using `dill` over `pickle`
    - auto-detects `isal` compression if available
    - allows for custom database naming
    - allows for database storage in cloudfs using `pathz_v2`

- `fuze_v1` is an attempt for native `FUSEv3` support. Currently WIP as Ubuntu 18.04 doesn't support `FUSEv3` easily

- `fuze_v2` utilizes native binaries to offer `FUSEv1` support.

- `pathz` is the `v1` implementation of bringing `PathLike` compatiability with all `Cloud Providers` utilizing `fsspec`. Will be superceded by `pathz_v2`

- `pathz_v2` is the likely successor.
    - built on a forked version of [aiopath](https://github.com/alexdelorenzo/aiopath) `python 3.9`
    - Async built around `anyio`
    - cloud support built on `fsspec` FileSystems
    - lazily inits to support underlying filesystem: `gcsfs`, `s3fs`
    - supports: `minio`, `s3`, `gs`
    - has both `sync` AND `async` support natively. Async functions are prepended with `async_[sync_name]`
    - Autodetects cloud auth using `lazy.configz.CloudAuthz`
    - offers both `posix` and `windows` support
    - utilizes `asynccontextmanager` for `async_open` wrapped around `anyio.AsyncFile`
    - can transparently access the `fsspec.FileSystem`
    - rewrites `provider.FileSystem` modules to share a common API for async. `_[func]` -> `async_[func]`
    - offers simple serialization/de-serialization APIs utilizing `lazy.serialize.Serializers`
        - [x] JSON
        - [x] JSONLines
        - [x] Pickle
        - [x] Text
        - [x] YAML
        - [ ] CSV
        - [ ] TSV

**Notes**:

- `pathz_v2.gs_gcp` has a inherit limitation in appending to file, if the changes are less than `262 kb`, then likely changes won't persist. [link](https://github.com/fsspec/gcsfs/issues/389)

