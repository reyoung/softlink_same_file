package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type fileInfo struct {
	filename string
	key      string
	size     int64
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func panicT[T any](v T, error error) T {
	panicErr(error)
	return v
}

func getFileMD5(fn string) []byte {
	hash := md5.New()
	f := panicT(os.Open(fn))
	defer f.Close()
	panicT(io.Copy(hash, f))
	return hash.Sum(nil)
}

func statDir(dir string, outCh chan<- *fileInfo, complete *sync.WaitGroup, minSize int) {
	defer complete.Done()
	for {
		var nextDirs []string
		entries := panicT(ioutil.ReadDir(dir))
		for _, stat := range entries {
			if stat.Mode()&fs.ModeSymlink != 0 {
				continue
			}
			fn := path.Join(dir, stat.Name())
			if stat.IsDir() {
				nextDirs = append(nextDirs, fn)
				continue
			}
			fSize := stat.Size()
			if fSize <= int64(minSize) {
				continue
			}

			sum := getFileMD5(fn)

			key := fmt.Sprintf("%d_%x", fSize, sum)
			outCh <- &fileInfo{
				filename: fn,
				key:      key,
				size:     fSize,
			}
		}
		if len(nextDirs) == 0 {
			break
		}
		if len(nextDirs) > 1 {
			complete.Add(len(nextDirs) - 1)
			for _, d := range nextDirs[1:] {
				go statDir(d, outCh, complete, minSize)
			}
		}
		dir = nextDirs[0]
	}
}

type SymlinkInfo struct {
	files []string
	size  int64
}

func doSoftLinkSameFile(dirs []string, dryRun bool, minSize int) {
	ch := make(chan *fileInfo, 64)
	var complete sync.WaitGroup
	complete.Add(len(dirs))
	for _, d := range dirs {
		go statDir(d, ch, &complete, minSize)
	}

	key2fn := make(map[string][]*fileInfo)

	var mapWait sync.WaitGroup
	mapWait.Add(1)
	go func() {
		defer mapWait.Done()
		for info := range ch {
			key2fn[info.key] = append(key2fn[info.key], info)
		}
	}()

	complete.Wait()
	close(ch)
	mapWait.Wait()

	var infos []SymlinkInfo

	for _, v := range key2fn {
		if len(v) == 1 {
			continue
		}
		var info SymlinkInfo
		for _, item := range v {
			info.files = append(info.files, item.filename)
		}
		info.size = v[0].size
		infos = append(infos, info)
	}
	key2fn = nil

	for _, info := range infos {
		fns := strings.Join(info.files, ",")
		savedSize := info.size * int64(len(infos)-1)
		fmt.Printf("Symlink %s, save bytes %d\n", fns, savedSize)
		if dryRun {
			continue
		}
		base := panicT(filepath.Abs(info.files[0]))

		for _, fn := range info.files[1:] {
			f := panicT(os.Stat(fn))
			mode := f.Mode()
			panicErr(os.Remove(fn))
			panicErr(os.Symlink(base, fn))
			panicErr(os.Chmod(fn, mode))
		}
	}

}

func main() {
	dryRun := flag.Bool("dry_run", false, "dry_run or not")
	dir := flag.String("dir", ".", "dir to soft link, default `.`, comma separated")
	minSize := flag.Int("min_size", 16*1024, "min size to softlink")
	flag.Parse()
	dirs := strings.Split(*dir, ",")
	if len(dirs) != 0 {
		doSoftLinkSameFile(dirs, *dryRun, *minSize)
	}
}
