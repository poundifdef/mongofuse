package main

import (
	"context"
	"encoding/json"
	"flag"
	"hash/fnv"
	"log"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type HelloRoot struct {
	fs.Inode
	MongoClient *mongo.Client
	Collection  string
	Document    string
}

// Ensure we are implementing the NodeReaddirer interface
var _ = (fs.NodeReaddirer)((*HelloRoot)(nil))

func _h(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

type MongoDoc struct {
	ID string `json:"_id" bson:"_id"`
}

// bytesFileHandle is a file handle that carries separate content for
// each Open call
type bytesFileHandle struct {
	content []byte
}

// Readdir is part of the NodeReaddirer interface
func (n *HelloRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	db := n.MongoClient.Database("gmail_deleter")

	if n.Collection == "" {
		collections, _ := db.ListCollectionNames(context.TODO(), &options.ListCollectionsOptions{})
		r := make([]fuse.DirEntry, 0, len(collections))
		for _, collection := range collections {
			d := fuse.DirEntry{
				Name: collection,
				Ino:  _h(collection),
				Mode: fuse.S_IFDIR,
			}
			r = append(r, d)
		}
		return fs.NewListDirStream(r), 0
	}
	if n.Document == "" {
		limit := int64(3)
		var docs []MongoDoc
		coll := db.Collection(n.Collection)
		opts := options.FindOptions{
			Projection: bson.M{"_id": 1},
			Limit:      &limit,
		}
		result, _ := coll.Find(context.TODO(), bson.M{}, &opts)
		result.All(context.TODO(), &docs)
		r := make([]fuse.DirEntry, 0, len(docs))
		for _, doc := range docs {
			d := fuse.DirEntry{
				Name: doc.ID + ".json",
				Ino:  _h(n.Collection + "/" + doc.ID + ".json"),
				Mode: fuse.S_IFDIR,
			}
			r = append(r, d)
		}
		return fs.NewListDirStream(r), 0
	}

	return fs.NewListDirStream([]fuse.DirEntry{}), 0
}

// Ensure we are implementing the NodeLookuper interface
var _ = (fs.NodeLookuper)((*HelloRoot)(nil))

// Lookup is part of the NodeLookuper interface
func (n *HelloRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// parent = blank, name = top level dir
	// parent = collection, name = x.json
	// log.Println("jay lookup")
	// log.Println(n.Path(nil))
	// log.Println(name)
	// i, err := strconv.Atoi(name)
	// if err != nil {
	// 	return nil, syscall.ENOENT
	// }

	// if i >= n.num || i <= 1 {
	// 	return nil, syscall.ENOENT
	// }

	stable := fs.StableAttr{
		//Mode: fuse.S_IFDIR,
		// The child inode is identified by its Inode number.
		// If multiple concurrent lookups try to find the same
		// inode, they are deduplicated on this key.
		//Ino: _h(name), //uint64(i),
	}

	parent := n.Path(nil)
	childName := name

	operations := &HelloRoot{
		MongoClient: n.MongoClient,
	}

	// TODO: actually look up these in mongo to see if they exist

	// root directory. listing collections.
	if parent == "" {
		db := n.MongoClient.Database("gmail_deleter")
		collections, _ := db.ListCollectionNames(context.TODO(), &options.ListCollectionsOptions{})
		exists := false
		for _, coll := range collections {
			if coll == childName {
				exists = true
			}
		}
		if exists == false {
			return nil, syscall.ENOENT
		}

		stable.Mode = fuse.S_IFDIR
		stable.Ino = _h(childName)
		operations.Collection = childName
	} else {
		// looking up an individual document
		stable.Mode = fuse.S_IFREG
		stable.Ino = _h(parent + "/" + childName)
		operations.Collection = parent
		operations.Document = childName
	}

	// The NewInode call wraps the `operations` object into an Inode.
	child := n.NewInode(ctx, operations, stable)

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child, 0
}

var _ = (fs.NodeAccesser)((*HelloRoot)(nil))

func (n *HelloRoot) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}

var _ = (fs.NodeMkdirer)((*HelloRoot)(nil))

func (n *HelloRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := n.Path(nil)
	if path != "" {
		return nil, syscall.EROFS
	}

	db := n.MongoClient.Database("gmail_deleter")
	db.CreateCollection(ctx, name)

	new := &HelloRoot{
		MongoClient: n.MongoClient,
		Collection:  name,
	}

	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  _h(name),
	}

	inode := n.NewInode(ctx, new, stable)
	return inode, 0

}

var _ = (fs.NodeCreater)((*HelloRoot)(nil))

func (n *HelloRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	log.Println("jay creating")
	// path := n.Path(nil)
	// if path == "" {
	// 	db := n.MongoClient.Database("gmail_deleter")
	// 	db.CreateCollection(ctx, name)

	// 	n := &HelloRoot{
	// 		MongoClient: n.MongoClient,
	// 		Collection:  name,
	// 	}

	// 	stable := fs.StableAttr{
	// 		Mode: fuse.S_IFDIR,
	// 		Ino:  _h(name),
	// 	}

	// 	inode := n.NewInode(ctx, n, stable)
	// 	return inode, nil, flags, 0
	// }
	return nil, nil, 0, syscall.EROFS
	// inode, fh, flags, errno := n.LoopbackNode.Create(ctx, name, flags, mode, out)
	// if errno == 0 {
	// 	wn := inode.Operations().(*WindowsNode)
	// 	wn.openCount++
	// }

}

var _ = (fs.NodeSetattrer)((*HelloRoot)(nil))

func (bn *HelloRoot) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return 0
}

// Implement handleless write.
var _ = (fs.NodeWriter)((*HelloRoot)(nil))

func (bn *HelloRoot) Write(ctx context.Context, fh fs.FileHandle, buf []byte, off int64) (uint32, syscall.Errno) {
	var b map[string]interface{}
	e := json.Unmarshal([]byte(buf), &b)
	if e != nil {
		log.Println(e)
		return 0, syscall.EROFS
	}
	db := bn.MongoClient.Database("gmail_deleter")
	coll := db.Collection(bn.Collection)
	_, e = coll.ReplaceOne(ctx, bson.M{"_id": bn.Document[:len(bn.Document)-5]}, b)
	if e != nil {
		log.Println(e)
		return 0, syscall.EROFS
	}
	return uint32(len(buf)), 0
}

var _ = (fs.NodeOpener)((*HelloRoot)(nil))

func (f *HelloRoot) Open(ctx context.Context, openFlags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// disallow writes
	//log.Println(openFlags & (syscall.O_RDWR | syscall.O_WRONLY))
	//if openFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
	//return nil, 0, syscall.EROFS
	//}
	canWrite := (openFlags & (syscall.O_RDWR | syscall.O_WRONLY)) != 0

	if f.Document == "" {
		return nil, 0, syscall.EROFS
	}

	if strings.HasSuffix(f.Document, ".json") == false {
		return nil, 0, syscall.EROFS
	}

	db := f.MongoClient.Database("gmail_deleter")
	collection := db.Collection(f.Collection)

	id := f.Document[:len(f.Document)-5]
	oid, e := primitive.ObjectIDFromHex(id)

	var doc *mongo.SingleResult

	if e == nil {
		doc = collection.FindOne(context.TODO(), bson.M{"_id": oid})
	} else {
		doc = collection.FindOne(context.TODO(), bson.M{"_id": id})
	}

	raw, e := doc.DecodeBytes()
	if e != nil && canWrite == false {
		return nil, 0, syscall.ENOENT
	}
	if e != nil && canWrite {
		collection.InsertOne(ctx, bson.M{"_id": id})
		doc = collection.FindOne(
			context.TODO(), bson.M{"_id": id},
		)
	}
	b, e := bson.MarshalExtJSONIndent(raw, true, false, "", "    ")

	fh = &bytesFileHandle{
		content: b,
	}

	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, 0
}

// bytesFileHandle allows reads
var _ = (fs.FileReader)((*bytesFileHandle)(nil))

func (fh *bytesFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}

	// We could copy to the `dest` buffer, but since we have a
	// []byte already, return that.
	return fuse.ReadResultData(fh.content[off:end]), 0
}

// func (r *HelloRoot) OnAdd(ctx context.Context) {
// 	ch := r.NewPersistentInode(
// 		ctx, &fs.MemRegularFile{
// 			Data: []byte("file.txt"),
// 			Attr: fuse.Attr{
// 				Mode: 0644,
// 			},
// 		}, fs.StableAttr{Ino: 2})
// 	r.AddChild("file.txt", ch, false)
// }

func (r *HelloRoot) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*HelloRoot)(nil))

// var _ = (fs.NodeOnAdder)((*HelloRoot)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	opts := &fs.Options{}
	opts.Debug = *debug

	mongoClient, err := mongo.Connect(
		context.TODO(),
		options.Client(), //.ApplyURI(db.ConnectionString),
	)
	if err != nil {
		log.Fatal(err)
	}
	//db.MongoClient = mongoClient

	root := &HelloRoot{
		MongoClient: mongoClient,
	}

	server, err := fs.Mount(flag.Arg(0), root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
