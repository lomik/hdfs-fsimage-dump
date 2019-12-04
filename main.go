package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/golang/protobuf/proto"

	pb "github.com/lomik/hdfs-fsimage-dump/pb/hadoop_hdfs_fsimage"
)

const (
	RootInodeID    = 16385
	DetachedPrefix = "(detached)"
	SnapshotPrefix = "(snapshot)"
	UnknownName    = "(unknown)"
)

var Codec string
var sectionMap map[string]*pb.FileSummary_Section

var (
	permMap = []string{
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

type IFrameReader interface {
	ReadFrame() ([]byte, error)
	ReadMessage(msg proto.Message) error
	ReadUvarint() (uint64, error)
}

func main() {

	var extraFieldsJson map[string]interface{}

	fileName := flag.String("i", "", "[mandatory]: HDFS fsimage filename")
	extraFields := flag.String("extra-fields", "", "[optional]: add static json fields =\"{\\\"Data\\\":\\\"2006-01-02\\\"\"}")
	snapReplace := flag.Bool("snap-replace", false, "[optional]: snapshots are placed into virtual directory /(snapshots)")
	snapCleanup := flag.Bool("snap-cleanup", false, "[optional]: snapshots will contain only deleted object(s)")

	flag.Parse()

	if *fileName == "" {
		flag.PrintDefaults()
		os.Exit(2)
	}

	if *extraFields != "" {
		err := json.Unmarshal([]byte(*extraFields), &extraFieldsJson)
		if err != nil {
			log.Fatal(err)
		}
	}

	fInfo, err := os.Stat(*fileName)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(*fileName)
	if err != nil {
		log.Fatal(err)
	}

	sectionMap, Codec, err = readSummary(f, fInfo.Size())
	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(sectionMap)

	tree := NewNodeTree()
	strings := make(map[uint32]string)
	inodeReference := NewNodeRefTree()

	if err = readStrings(sectionMap["STRING_TABLE"], f, strings); err != nil {
		log.Fatal(err)
	}
	if err = readReferenceTree(sectionMap["INODE_REFERENCE"], f, inodeReference); err != nil {
		log.Fatal(err)
	}
	if err = readTree(sectionMap["INODE_DIR"], f, tree, inodeReference); err != nil {
		log.Fatal(err)
	}
	if err = readSnapshotDiff(sectionMap["SNAPSHOT_DIFF"], f, tree, inodeReference); err != nil {
		log.Fatal(err)
	}
	if err = readDirectoryNames(sectionMap["INODE"], f, tree); err != nil {
		log.Fatal(err)
	}
	if err = dumpSnapshots(sectionMap["SNAPSHOT"], f, tree, snapReplace); err != nil {
		log.Fatal(err)
	}
	if err = dump(sectionMap["INODE"], f, tree, strings, extraFieldsJson, snapCleanup); err != nil {
		log.Fatal(err)
	}

	f.Close()
}

func readSummary(imageFile *os.File, fileLength int64) (map[string]*pb.FileSummary_Section, string, error) {

	_, err := imageFile.Seek(-4, 2)
	if err != nil {
		return nil, "", err
	}

	var summaryLength int32
	if err = binary.Read(imageFile, binary.BigEndian, &summaryLength); err != nil {
		return nil, "", err
	}

	fr, err := NewFrameReader(imageFile, fileLength-int64(summaryLength)-4, int64(summaryLength))
	if err != nil {
		return nil, "", err
	}

	fileSummary := &pb.FileSummary{}
	if err = fr.ReadMessage(fileSummary); err != nil {
		return nil, "", err
	}
	var codec string
	if fileSummary.Codec == nil {
		//fmt.Printf("readSummary codec: %v\n", "nil")
		codec = ""
	} else {
		//fmt.Printf("readSummary codec: %v\n", *fileSummary.Codec)
		codec = *fileSummary.Codec
	}

	sectionMap := make(map[string]*pb.FileSummary_Section)
	for _, value := range fileSummary.GetSections() {
		// fmt.Println("section", value.GetName())
		sectionMap[value.GetName()] = value
	}

	fr = nil
	return sectionMap, codec, nil
}

func dumpSnapshots(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, snapReplace *bool) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
	if err != nil {
		return err
	}

	snapshotSection := &pb.SnapshotSection{}
	if err = fr.ReadMessage(snapshotSection); err != nil {
		return err
	}

	snapshot := &pb.SnapshotSection_Snapshot{}

	for {
		if err = fr.ReadMessage(snapshot); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if snapshot.GetRoot().Directory != nil {
			if *snapReplace {
				t := true
				paths := getPaths(snapshot.GetRoot().GetId(), string(snapshot.GetRoot().GetName()), tree, true, &t)
				snapName := fmt.Sprintf("%s/%s%s", SnapshotPrefix, string(snapshot.GetRoot().GetName()), paths[0])
				tree.SetParentName(snapshot.GetRoot().GetId(), snapshot.GetSnapshotId(), RootInodeID, []byte(snapName))
			} else {
				ps := tree.GetParents(snapshot.GetRoot().GetId())
				snapName := fmt.Sprintf("%s/.snapshot/%s", ps[0].Name, string(snapshot.GetRoot().GetName()))
				tree.SetParentName(snapshot.GetRoot().GetId(), snapshot.GetSnapshotId(), ps[0].Parent, []byte(snapName))
			}
		}
	}

	fr = nil
	return nil
}

func readSnapshotDiff(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, inodeReference *NodeRefTree) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
	if err != nil {
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
			for _, deletedInodeRef := range snapshotDirDiff.GetDeletedINodeRef() {
				tree.SetParent(inodeReference.GetRefId(deletedInodeRef), snapshotDirDiff.GetSnapshotId(), snapshotDiff.GetInodeId())
				refName := inodeReference.GetRefName(deletedInodeRef)
				if len(refName) > 0 {
					tree.SetName(inodeReference.GetRefId(deletedInodeRef), snapshotDirDiff.GetSnapshotId(), refName)
				}
			}

			for _, deletedInode := range snapshotDirDiff.GetDeletedINode() {
				tree.SetParent(deletedInode, snapshotDirDiff.GetSnapshotId(), snapshotDiff.GetInodeId())
				if len(snapshotDirDiff.GetName()) > 0 {
					tree.SetName(deletedInode, snapshotDirDiff.GetSnapshotId(), snapshotDirDiff.GetName())
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

	fr = nil
	return nil
}

func readDirectoryNames(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
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
			tree.SetName(inode.GetId(), 0, inode.GetName())
		}
	}

	fr = nil
	return nil
}

func readTree(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, inodeReference *NodeRefTree) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
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
			tree.SetParent(children[j], 0, dirEntry.GetParent())
		}

		// children that are reference nodes, each element is a reference node id
		refChildren := dirEntry.GetRefChildren()
		for j := 0; j < len(refChildren); j++ {
			tree.SetParent(inodeReference.GetRefId(refChildren[j]), inodeReference.GetRefSnapId(refChildren[j]), dirEntry.GetParent())
		}
	}

	fr = nil
	return nil
}

func readReferenceTree(info *pb.FileSummary_Section, imageFile *os.File, inodeReference *NodeRefTree) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
	if err != nil {
		return err
	}

	inodeReferenceSection := &pb.INodeReferenceSection_INodeReference{}

	i := uint32(0)
	for {
		if err = fr.ReadMessage(inodeReferenceSection); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		inodeReference.SetRefSnapName(i, inodeReferenceSection.GetLastSnapshotId(),
			inodeReferenceSection.GetReferredId(), inodeReferenceSection.GetName())
		i++
	}

	fr = nil
	return nil
}

func readStrings(info *pb.FileSummary_Section, imageFile *os.File, strings map[uint32]string) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
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

	fr = nil
	return nil
}

func dump(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree,
	strings map[uint32]string, extraFields map[string]interface{}, snapCleanup *bool) error {

	var fr IFrameReader
	var err error

	if Codec == "" {
		fr, err = NewFrameReader(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	} else {
		fr, err = NewFrameReader2(imageFile, int64(info.GetOffset()), int64(info.GetLength()), Codec)
	}
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

		if inode.File != nil {
			isDir := false
			paths := getPaths(inode.GetId(), string(inode.GetName()), tree, isDir, snapCleanup)
			blocks := inode.File.GetBlocks()
			size := uint64(0)
			for i := 0; i < len(blocks); i++ {
				size += blocks[i].GetNumBytes()
			}
			perm := inode.File.GetPermission() % (1 << 16)
			dataDump := map[string]interface{}{
				"Replication":        inode.File.GetReplication(),
				"ModificationTime":   time.Unix(0, int64(inode.File.GetModificationTime())*1e6).Format("2006-01-02 15:04:05"),
				"ModificationTimeMs": inode.File.GetModificationTime(),
				"AccessTime":         time.Unix(0, int64(inode.File.GetAccessTime())*1e6).Format("2006-01-02 15:04:05"),
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

			if len(paths) == 0 && inode.GetId() != RootInodeID {
				paths = append(paths, fmt.Sprintf("/%s/%s", UnknownName, string(inode.GetName())))
			}
			for _, path := range paths {
				dataDump["Path"] = fmt.Sprintf("%s", path)
				jsonEncoder.Encode(dataDump)
			}
		}

		if inode.Directory != nil {
			isDir := true
			paths := getPaths(inode.GetId(), string(inode.GetName()), tree, isDir, snapCleanup)
			perm := inode.Directory.GetPermission() % (1 << 16)
			dataDump := map[string]interface{}{
				"ModificationTime":   time.Unix(0, int64(inode.Directory.GetModificationTime())*1e6).Format("2006-01-02 15:04:05"),
				"ModificationTimeMs": inode.Directory.GetModificationTime(),
				"User":               strings[uint32(inode.Directory.GetPermission()>>40)],
				"Group":              strings[uint32((inode.Directory.GetPermission()>>16)%(1<<24))],
				"Permission":         fmt.Sprintf("d%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
				// "RawPermission":    inode.Directory.GetPermission(),
			}
			for k, v := range extraFields {
				dataDump[k] = v
			}
			if len(paths) == 0 && !*snapCleanup && inode.GetId() != RootInodeID {
				paths = append(paths, fmt.Sprintf("/%s/%s", UnknownName, string(inode.GetName())))
			}
			for _, path := range paths {
				dataDump["Path"] = fmt.Sprintf("%s", path)
				jsonEncoder.Encode(dataDump)
			}
		}
	}

	fr = nil
	return nil
}
