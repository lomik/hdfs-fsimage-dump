package main

import (
	pb "./pb/hadoop_hdfs_fsimage"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"os"
	"time"
)

const (
	RootInodeID    = 16385
	DetachedPrefix = "/detached/"
)

var (
	UnknownName = "(unknown)"
	permMap     = []string{
		"---",
		"--x",
		"-w-",
		"-wx",
		"r--",
		"r-x",
		"rw-",
		"rwx",
	}
)

func main() {
	var fileName string
	var extraFields string
	var extraFieldsJson map[string]interface{}

	flag.StringVar(&fileName, "i", "", "[mandatory]: HDFS fsimage filename")
	flag.StringVar(&extraFields, "extra-fields", "", "[optional]: add static json fields =\"{\\\"Data\\\":\\\"2006-01-02\\\"\"}")
	flag.Parse()

	if fileName == "" {
		flag.PrintDefaults()
		os.Exit(2)
	}

	if extraFields != "" {
		err := json.Unmarshal([]byte(extraFields), &extraFieldsJson)
		if err != nil {
			log.Fatal(err)
		}
	}

	fInfo, err := os.Stat(fileName)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}

	sectionMap, err := readSummary(f, fInfo.Size())
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(sectionMap)

	tree := NewNodeTree()
	strings := make(map[uint32]string)
	snaptree := NewNodeTree()

	if err = readStrings(sectionMap["STRING_TABLE"], f, strings); err != nil {
		log.Fatal(err)
	}
	if err = readTree(sectionMap["INODE_DIR"], f, tree); err != nil {
		log.Fatal(err)
	}
	if err = readDirectoryNames(sectionMap["INODE"], f, tree); err != nil {
		log.Fatal(err)
	}
	if err = dumpSnapshots(sectionMap["SNAPSHOT"], f, tree, snaptree, strings, extraFieldsJson); err != nil {
		log.Fatal(err)
	}
	if err = readSnapshotDiff(sectionMap["SNAPSHOT_DIFF"], f, snaptree); err != nil {
		log.Fatal(err)
	}
	if err = dump(sectionMap["INODE"], f, tree, snaptree, strings, extraFieldsJson); err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func readSummary(imageFile *os.File, fileLength int64) (map[string]*pb.FileSummary_Section, error) {
	_, err := imageFile.Seek(-4, 2)
	if err != nil {
		return nil, err
	}

	var summaryLength int32
	if err = binary.Read(imageFile, binary.BigEndian, &summaryLength); err != nil {
		return nil, err
	}

	fr, err := NewFrameReader(imageFile, fileLength-int64(summaryLength)-4, int64(summaryLength))
	if err != nil {
		return nil, err
	}

	fileSummary := &pb.FileSummary{}
	if err = fr.ReadMessage(fileSummary); err != nil {
		return nil, err
	}

	sectionMap := make(map[string]*pb.FileSummary_Section)
	for _, value := range fileSummary.GetSections() {
		// fmt.Println("section", value.GetName())
		sectionMap[value.GetName()] = value
	}

	return sectionMap, nil
}

func dumpSnapshots(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, snaptree *NodeTree, strings map[uint32]string, extraFields map[string]interface{}) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	snapshotSection := &pb.SnapshotSection{}
	if err = fr.ReadMessage(snapshotSection); err != nil {
		return err
	}

	jsonEncoder := json.NewEncoder(os.Stdout)
	snapshot := &pb.SnapshotSection_Snapshot{}

	for {
		if err = fr.ReadMessage(snapshot); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if snapshot.GetRoot().Directory != nil {
			snapName := fmt.Sprintf(".snapshot/%s", string(snapshot.GetRoot().GetName()))
			snaptree.SetParentName(uint64(snapshot.GetSnapshotId()), snapshot.GetRoot().GetId(), []byte(snapName))

			path := getPath(uint64(snapshot.GetSnapshotId()), tree, snaptree)
			perm := snapshot.GetRoot().Directory.GetPermission() % (1 << 16)
			dataDump := map[string]interface{}{
				"Path":               fmt.Sprintf("%s%s", path, snapName),
				"ModificationTime":   time.Unix(int64(snapshot.GetRoot().Directory.GetModificationTime()/1000), 0).Format("2006-01-02 15:04:05"),
				"ModificationTimeMs": snapshot.GetRoot().Directory.GetModificationTime(),
				"User":               strings[uint32(snapshot.GetRoot().Directory.GetPermission()>>40)],
				"Group":              strings[uint32((snapshot.GetRoot().Directory.GetPermission()>>16)%(1<<24))],
				"Permission":         fmt.Sprintf("d%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
				// "RawPermission":    snapshot.GetRoot().Directory.GetPermission(),
			}
			for k, v := range extraFields {
				dataDump[k] = v
			}
			jsonEncoder.Encode(dataDump)
		}

		if snapshot.GetRoot().File != nil {
			log.Fatal("snapshot must be a directory")
		}
	}

	return nil
}

func readSnapshotDiff(info *pb.FileSummary_Section, imageFile *os.File, snaptree *NodeTree) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	snapshotDiffSection := &pb.SnapshotDiffSection{}
	if err = fr.ReadMessage(snapshotDiffSection); err != nil {
		return err
	}

	snapshotDiff := &pb.SnapshotDiffSection_DiffEntry{}
	snapshotDirDiff := &pb.SnapshotDiffSection_DirectoryDiff{}
	snapshotFileDiff := &pb.SnapshotDiffSection_FileDiff{}
	snapshotCreatedListEntry := &pb.SnapshotDiffSection_CreatedListEntry{}

	for {
		body, err := fr.ReadFrame()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err = proto.Unmarshal(body, snapshotDiff); err != nil {
			return err
		}

		for i := 0; i < int(snapshotDiff.GetNumOfDiff()); i++ {

			// read and skip FILEDIFF entry
			if snapshotDiff.GetType() == pb.SnapshotDiffSection_DiffEntry_FILEDIFF {
				if err = fr.ReadMessage(snapshotFileDiff); err != nil {
					return err
				}
				continue
			}

			body, err := fr.ReadFrame()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			if err = proto.Unmarshal(body, snapshotDirDiff); err != nil {
				return err
			}

			for _, deletedInode := range snapshotDirDiff.GetDeletedINode() {
				snaptree.SetParent(deletedInode, uint64(snapshotDirDiff.GetSnapshotId()))
				if snapshotDirDiff.GetIsSnapshotRoot() == false {
					snaptree.SetName(deletedInode, snapshotDirDiff.GetName())
				}
			}

			// read and skip CreatedList
			for j := 0; j < int(snapshotDirDiff.GetCreatedListSize()); j++ {
				if err = fr.ReadMessage(snapshotCreatedListEntry); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func readDirectoryNames(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	inodeSection := &pb.INodeSection{}
	if err = fr.ReadMessage(inodeSection); err != nil {
		return err
	}

	inode := &pb.INodeSection_INode{}
	for {
		body, err := fr.ReadFrame()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// skip files without parse
		if len(body) >= 2 && body[0] == 0x8 && body[1] == 0x1 {
			continue
		}

		if err = proto.Unmarshal(body, inode); err != nil {
			return err
		}

		if inode.GetDirectory() != nil {
			tree.SetName(inode.GetId(), inode.GetName())
		}
	}

	return nil
}

func readTree(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	dirEntry := &pb.INodeDirectorySection_DirEntry{}
	for {
		if err = fr.ReadMessage(dirEntry); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		children := dirEntry.GetChildren()
		for j := 0; j < len(children); j++ {
			tree.SetParent(children[j], dirEntry.GetParent())
		}
	}
	return nil
}

func readStrings(info *pb.FileSummary_Section, imageFile *os.File, strings map[uint32]string) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	stringTableSection := &pb.StringTableSection{}
	if err = fr.ReadMessage(stringTableSection); err != nil {
		return err
	}

	entry := &pb.StringTableSection_Entry{}
	for {
		if err = fr.ReadMessage(entry); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		strings[entry.GetId()] = entry.GetStr()
	}

	return nil
}

func dump(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, snaptree *NodeTree, strings map[uint32]string, extraFields map[string]interface{}) error {
	fr, err := NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	if err != nil {
		return err
	}

	inodeSection := &pb.INodeSection{}
	if err = fr.ReadMessage(inodeSection); err != nil {
		return err
	}

	inode := &pb.INodeSection_INode{}
	jsonEncoder := json.NewEncoder(os.Stdout)

	for {
		if err = fr.ReadMessage(inode); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Path    Replication     ModificationTime        AccessTime      PreferredBlockSize      BlocksCount     FileSize        NSQUOTA DSQUOTA Permission      UserName        GroupName

		path := getPath(inode.GetId(), tree, snaptree)

		if inode.File != nil {
			blocks := inode.File.GetBlocks()
			size := uint64(0)
			for i := 0; i < len(blocks); i++ {
				size += blocks[i].GetNumBytes()
			}
			perm := inode.File.GetPermission() % (1 << 16)
			dataDump := map[string]interface{}{
				"Path":               fmt.Sprintf("%s%s", path, string(inode.GetName())),
				"Replication":        inode.File.GetReplication(),
				"ModificationTime":   time.Unix(int64(inode.File.GetModificationTime()/1000), 0).Format("2006-01-02 15:04:05"),
				"ModificationTimeMs": inode.File.GetModificationTime(),
				"AccessTime":         time.Unix(int64(inode.File.GetAccessTime()/1000), 0).Format("2006-01-02 15:04:05"),
				"AccessTimeMs":       inode.File.GetAccessTime(),
				"PreferredBlockSize": inode.File.GetPreferredBlockSize(),
				"BlocksCount":        len(blocks),
				"FileSize":           size,
				"User":               strings[uint32(inode.File.GetPermission()>>40)],
				"Group":              strings[uint32((inode.File.GetPermission()>>16)%(1<<24))],
				"Permission":         fmt.Sprintf("-%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
				// "RawPermission":      inode.File.GetPermission(),
			}
			for k, v := range extraFields {
				dataDump[k] = v
			}
			jsonEncoder.Encode(dataDump)
		}

		if inode.Directory != nil {
			perm := inode.Directory.GetPermission() % (1 << 16)
			dataDump := map[string]interface{}{
				"Path":               fmt.Sprintf("%s%s", path, string(inode.GetName())),
				"ModificationTime":   time.Unix(int64(inode.Directory.GetModificationTime()/1000), 0).Format("2006-01-02 15:04:05"),
				"ModificationTimeMs": inode.Directory.GetModificationTime(),
				"User":               strings[uint32(inode.Directory.GetPermission()>>40)],
				"Group":              strings[uint32((inode.Directory.GetPermission()>>16)%(1<<24))],
				"Permission":         fmt.Sprintf("d%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
				// "RawPermission":    inode.Directory.GetPermission(),
			}
			for k, v := range extraFields {
				dataDump[k] = v
			}
			jsonEncoder.Encode(dataDump)
		}
	}

	return nil
}
