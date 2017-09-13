package main

import (
	pb "./pb/hadoop_hdfs_fsimage"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"os"
)

const (
	RootInodeID    = 16385
	DetachedPrefix = "/detached/"
)

var (
	UnknownName = []byte("(unknown)")
)

func main() {
	fileName := os.Args[1]
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

	if err = readStrings(sectionMap["STRING_TABLE"], f, strings); err != nil {
		log.Fatal(err)
	}
	if err = readTree(sectionMap["INODE_DIR"], f, tree); err != nil {
		log.Fatal(err)
	}
	if err = readDirectoryNames(sectionMap["INODE"], f, tree); err != nil {
		log.Fatal(err)
	}
	if err = dump(sectionMap["INODE"], f, tree, strings); err != nil {
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

func dump(info *pb.FileSummary_Section, imageFile *os.File, tree *NodeTree, strings map[uint32]string) error {
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

	permMap := []string{
		"---",
		"--x",
		"-w-",
		"-wx",
		"r--",
		"r-x",
		"rw-",
		"rwx",
	}

	for {
		if err = fr.ReadMessage(inode); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Path    Replication     ModificationTime        AccessTime      PreferredBlockSize      BlocksCount     FileSize        NSQUOTA DSQUOTA Permission      UserName        GroupName
		path := tree.GetPath(inode.GetId())

		if inode.File != nil {
			blocks := inode.File.GetBlocks()
			size := uint64(0)
			for i := 0; i < len(blocks); i++ {
				size += blocks[i].GetNumBytes()
			}
			perm := inode.File.GetPermission() % (1 << 16)
			jsonEncoder.Encode(map[string]interface{}{
				"Path":               fmt.Sprintf("%s%s", path, string(inode.GetName())),
				"Replication":        inode.File.GetReplication(),
				"ModificationTime":   inode.File.GetModificationTime(),
				"AccessTime":         inode.File.GetAccessTime(),
				"PreferredBlockSize": inode.File.GetPreferredBlockSize(),
				"BlocksCount":        len(blocks),
				"FileSize":           size,
				// "RawPermission":      inode.File.GetPermission(),
				"User":               strings[uint32(inode.File.GetPermission()>>40)],
				"Group":              strings[uint32((inode.File.GetPermission()>>16)%(1<<24))],
				"Permission":         fmt.Sprintf("-%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
			})
		}

		if inode.Directory != nil {
			perm := inode.Directory.GetPermission() % (1 << 16)
			jsonEncoder.Encode(map[string]interface{}{
				"Path":             fmt.Sprintf("%s%s", path, string(inode.GetName())),
				"ModificationTime": inode.Directory.GetModificationTime(),
				// "RawPermission":    inode.Directory.GetPermission(),
				"User":             strings[uint32(inode.Directory.GetPermission()>>40)],
				"Group":            strings[uint32((inode.Directory.GetPermission()>>16)%(1<<24))],
				"Permission":       fmt.Sprintf("-%s%s%s", permMap[(perm>>6)%8], permMap[(perm>>3)%8], permMap[(perm)%8]),
			})
		}
	}

	return nil
}
