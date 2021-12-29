This is a rudimentary go program that allows you to mount a mongo database
as a FUSE file system. This is intended to make it easier to read/write JSON
from scripts into MongoDB. 

This does not currently support saving non-JSON files (eg, GridFS) in Mongo.

From the mountpoint, you have 2 directory levels:

```
./mnt
├── collection1
│   ├── doc1.json
│   └── doc2.json
├── collection2
│   ├── 61c127da8758291865ba5b0f.json
│   ├── 61c127da8758291865ba5b10.json
│   └── 61c127da8758291865ba5b11.json
```

Each folder represents a mongo collection, and each JSON file is a mongo document.

You may create new collections (folders), documents (json files), and you may modify
existing json documents.