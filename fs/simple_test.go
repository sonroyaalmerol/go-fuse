// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/internal/testutil"
	"github.com/hanwen/go-fuse/v2/posixtest"
	"github.com/kylelemons/godebug/pretty"
	"golang.org/x/sys/unix"
)

type testCase struct {
	*testing.T

	dir     string
	origDir string
	mntDir  string

	loopback InodeEmbedder
	rawFS    fuse.RawFileSystem
	server   *fuse.Server
}

// writeOrig writes a file into the backing directory of the loopback mount
func (tc *testCase) writeOrig(path, content string, mode os.FileMode) {
	if err := os.WriteFile(filepath.Join(tc.origDir, path), []byte(content), mode); err != nil {
		tc.Fatal(err)
	}
}

func (tc *testCase) clean() {
	if err := tc.server.Unmount(); err != nil {
		tc.Fatal(err)
	}
}

type testOptions struct {
	entryCache        bool
	enableLocks       bool
	attrCache         bool
	suppressDebug     bool
	testDir           string
	ro                bool
	directMount       bool // sets MountOptions.DirectMount
	directMountStrict bool // sets MountOptions.DirectMountStrict
	disableSplice     bool // sets MountOptions.DisableSplice
	idMappedMount     bool // sets MountOptions.IDMappedMount
}

// newTestCase creates the directories `orig` and `mnt` inside a temporary
// directory and mounts a loopback filesystem, backed by `orig`, on `mnt`.
func newTestCase(t *testing.T, opts *testOptions) *testCase {
	if opts == nil {
		opts = &testOptions{}
	}
	if opts.testDir == "" {
		opts.testDir = t.TempDir()
	}
	tc := &testCase{
		dir: opts.testDir,
		T:   t,
	}
	tc.origDir = tc.dir + "/orig"
	tc.mntDir = tc.dir + "/mnt"
	if err := os.Mkdir(tc.origDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(tc.mntDir, 0755); err != nil {
		t.Fatal(err)
	}

	var err error
	tc.loopback, err = NewLoopbackRoot(tc.origDir)
	if err != nil {
		t.Fatalf("NewLoopback: %v", err)
	}

	oneSec := time.Second

	attrDT := &oneSec
	if !opts.attrCache {
		attrDT = nil
	}
	entryDT := &oneSec
	if !opts.entryCache {
		entryDT = nil
	}
	tc.rawFS = NewNodeFS(tc.loopback, &Options{
		EntryTimeout: entryDT,
		AttrTimeout:  attrDT,
		Logger:       log.New(os.Stderr, "", 0),
	})

	mOpts := &fuse.MountOptions{
		DirectMount:       opts.directMount,
		DirectMountStrict: opts.directMountStrict,
		EnableLocks:       opts.enableLocks,
		DisableSplice:     opts.disableSplice,
		IDMappedMount:     opts.idMappedMount,
	}
	if !opts.suppressDebug {
		mOpts.Debug = testutil.VerboseTest()
	}
	if opts.ro {
		mOpts.Options = append(mOpts.Options, "ro")
	}
	if opts.idMappedMount {
		mOpts.Options = append(mOpts.Options, "default_permissions")
	}
	tc.server, err = fuse.NewServer(tc.rawFS, tc.mntDir, mOpts)
	if err != nil {
		t.Fatal(err)
	}

	go tc.server.Serve()
	if err := tc.server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tc.clean)

	return tc
}

func TestBasic(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})

	tc.writeOrig("file", "hello", 0644)

	fn := tc.mntDir + "/file"
	fi, err := os.Lstat(fn)
	if err != nil {
		t.Fatalf("Lstat: %v", err)
	}

	if fi.Size() != 5 {
		t.Errorf("got size %d want 5", fi.Size())
	}

	stat := fuse.ToStatT(fi)
	if got, want := uint32(stat.Mode), uint32(fuse.S_IFREG|0644); got != want {
		t.Errorf("got mode %o, want %o", got, want)
	}

	if err := os.Remove(fn); err != nil {
		t.Errorf("Remove: %v", err)
	}

	if fi, err := os.Lstat(fn); err == nil {
		t.Errorf("Lstat after remove: got file %v", fi)
	}
}

func TestFileFdLeak(t *testing.T) {
	tc := newTestCase(t, &testOptions{
		suppressDebug: false,
		attrCache:     true,
		entryCache:    true,
	})

	posixtest.FdLeak(t, tc.mntDir)

	tc.clean()
	bridge := tc.rawFS.(*rawBridge)
	tc = nil

	// posixtest.FdLeak also uses 15 as a limit.
	if got, want := len(bridge.files), 15; got > want {
		t.Errorf("found %d used file handles, should be <= %d", got, want)
	}
}

func TestNotifyEntry(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})

	orig := tc.origDir + "/file"
	fn := tc.mntDir + "/file"
	tc.writeOrig("file", "hello", 0644)

	st := syscall.Stat_t{}
	if err := syscall.Lstat(fn, &st); err != nil {
		t.Fatalf("Lstat before: %v", err)
	}

	if err := os.Remove(orig); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	after := syscall.Stat_t{}
	if err := syscall.Lstat(fn, &after); err != nil {
		t.Fatalf("Lstat after: %v", err)
	} else if !reflect.DeepEqual(st, after) {
		t.Fatalf("got after %#v, want %#v", after, st)
	}

	if errno := tc.loopback.EmbeddedInode().NotifyEntry("file"); errno != 0 {
		t.Errorf("notify failed: %v", errno)
	}

	if err := syscall.Lstat(fn, &after); err != syscall.ENOENT {
		t.Fatalf("Lstat after: got %v, want ENOENT", err)
	}
}

func TestReadDirStress(t *testing.T) {
	tc := newTestCase(t, &testOptions{suppressDebug: true, attrCache: true, entryCache: true})

	// Create 110 entries
	for i := 0; i < 110; i++ {
		name := fmt.Sprintf("file%036x", i)
		if err := os.WriteFile(filepath.Join(tc.mntDir, name), []byte("hello"), 0644); err != nil {
			t.Fatalf("WriteFile %q: %v", name, err)
		}
	}

	var wg sync.WaitGroup
	stress := func(gr int) {
		defer wg.Done()
		for i := 1; i < 100; i++ {
			f, err := os.Open(tc.mntDir)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = f.Readdirnames(-1)
			f.Close()
			if err != nil {
				t.Errorf("goroutine %d iteration %d: %v", gr, i, err)
				return
			}
		}
	}

	n := 3
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go stress(i)
	}
	wg.Wait()

}

// This test is racy. If an external process consumes space while this
// runs, we may see spurious differences between the two statfs() calls.
func TestStatFs(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: true, entryCache: true})

	empty := syscall.Statfs_t{}
	orig := empty
	if err := syscall.Statfs(tc.origDir, &orig); err != nil {
		t.Fatal("statfs orig", err)
	}

	mnt := syscall.Statfs_t{}
	if err := syscall.Statfs(tc.mntDir, &mnt); err != nil {
		t.Fatal("statfs mnt", err)
	}

	var mntFuse, origFuse fuse.StatfsOut
	mntFuse.FromStatfsT(&mnt)
	origFuse.FromStatfsT(&orig)

	if !reflect.DeepEqual(mntFuse, origFuse) {
		t.Errorf("Got %#v, want %#v", mntFuse, origFuse)
	}
}

func TestGetAttrParallel(t *testing.T) {
	// We grab a file-handle to provide to the API so rename+fstat
	// can be handled correctly. Here, test that closing and
	// (f)stat in parallel don't lead to fstat on closed files.
	// We can only test that if we switch off caching
	tc := newTestCase(t, &testOptions{suppressDebug: true})

	N := 100

	var fds []int
	var fns []string
	for i := 0; i < N; i++ {
		fn := fmt.Sprintf("file%d", i)
		tc.writeOrig(fn, "ello", 0644)
		fn = filepath.Join(tc.mntDir, fn)
		fns = append(fns, fn)
		fd, err := syscall.Open(fn, syscall.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}

		fds = append(fds, fd)
	}

	var wg sync.WaitGroup
	wg.Add(2 * N)
	for i := 0; i < N; i++ {
		go func(i int) {
			if err := syscall.Close(fds[i]); err != nil {
				t.Errorf("close %d: %v", i, err)
			}
			wg.Done()
		}(i)
		go func(i int) {
			var st syscall.Stat_t
			if err := syscall.Lstat(fns[i], &st); err != nil {
				t.Errorf("lstat %d: %v", i, err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestMknod(t *testing.T) {
	tc := newTestCase(t, &testOptions{})

	modes := map[string]uint32{
		"regular": syscall.S_IFREG,
		"socket":  syscall.S_IFSOCK,
		"fifo":    syscall.S_IFIFO,
	}

	for nm, mode := range modes {
		t.Run(nm, func(t *testing.T) {
			p := filepath.Join(tc.mntDir, nm)
			err := syscall.Mknod(p, mode|0755, (8<<8)|0)
			if err != nil {
				t.Fatalf("mknod(%s): %v", nm, err)
			}

			var st syscall.Stat_t
			if err := syscall.Stat(p, &st); err != nil {
				got := st.Mode &^ 07777
				if want := uint(mode); want != uint(got) {
					t.Fatalf("stat(%s): got %o want %o", nm, got, want)
				}
			}

			// We could test if the files can be
			// read/written but: The kernel handles FIFOs
			// without talking to FUSE at all. Presumably,
			// this also holds for sockets.  Regular files
			// are tested extensively elsewhere.
		})
	}
}

func TestMknodNotSupported(t *testing.T) {
	mountPoint := t.TempDir()

	server, err := Mount(mountPoint, &Inode{}, nil)
	if err != nil {
		t.Fatalf("cannot mount: %v", err)
	}

	defer server.Unmount()

	name := filepath.Join(mountPoint, "foo")

	if got, want := syscall.Mknod(name, syscall.S_IFREG|0755, (8<<8)|0), syscall.ENOTSUP; got != want {
		t.Fatalf("mknod: got %v, want %v", got, want)
	}
}

func TestPosix(t *testing.T) {
	noisy := map[string]bool{
		"ParallelFileOpen": true,
		"ReadDir":          true,
	}

	for nm, fn := range posixtest.All {
		t.Run(nm, func(t *testing.T) {
			tc := newTestCase(t, &testOptions{
				suppressDebug: noisy[nm],
				attrCache:     true,
				entryCache:    true,
				enableLocks:   true,
			})

			fn(t, tc.mntDir)
		})
	}
}

func TestReadDisableSplice(t *testing.T) {
	tc := newTestCase(t, &testOptions{
		disableSplice: true,
	})

	posixtest.FileBasic(t, tc.mntDir)
}

func TestOpenDirectIO(t *testing.T) {
	// Apparently, tmpfs does not allow O_DIRECT, so try to create
	// a test temp directory in /var/tmp.
	ext4Dir, err := os.MkdirTemp("/var/tmp", "go-fuse.TestOpenDirectIO")
	if err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(ext4Dir) })

	posixtest.DirectIO(t, ext4Dir)
	if t.Failed() {
		t.Skip("DirectIO failed on underlying FS")
	}

	opts := testOptions{
		testDir:    ext4Dir,
		attrCache:  true,
		entryCache: true,
	}

	tc := newTestCase(t, &opts)
	posixtest.DirectIO(t, tc.mntDir)
}

// TestFsstress is loosely modeled after xfstest's fsstress. It performs rapid
// parallel removes / creates / readdirs. Coupled with inode reuse, this test
// used to deadlock go-fuse quite quickly.
//
// Note: Run as
//
//	TMPDIR=/var/tmp go test -run TestFsstress
//
// to make sure the backing filesystem is ext4. /tmp is tmpfs on modern Linux
// distributions, and tmpfs does not reuse inode numbers, hiding the problem.
func TestFsstress(t *testing.T) {
	tc := newTestCase(t, &testOptions{suppressDebug: true, attrCache: true, entryCache: true})

	{
		old := runtime.GOMAXPROCS(100)
		defer runtime.GOMAXPROCS(old)
	}

	const concurrency = 10
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// operations taking 1 path argument
	ops1 := map[string]func(string) error{
		"mkdir":      func(p string) error { return syscall.Mkdir(p, 0700) },
		"rmdir":      func(p string) error { return syscall.Rmdir(p) },
		"mknod_reg":  func(p string) error { return syscall.Mknod(p, 0700|syscall.S_IFREG, 0) },
		"remove":     os.Remove,
		"unlink":     syscall.Unlink,
		"mknod_sock": func(p string) error { return syscall.Mknod(p, 0700|syscall.S_IFSOCK, 0) },
		"mknod_fifo": func(p string) error { return syscall.Mknod(p, 0700|syscall.S_IFIFO, 0) },
		"mkfifo":     func(p string) error { return syscall.Mkfifo(p, 0700) },
		"symlink":    func(p string) error { return syscall.Symlink("foo", p) },
		"creat": func(p string) error {
			fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_EXCL, 0700)
			if err == nil {
				syscall.Close(fd)
			}
			return err
		},
	}
	// operations taking 2 path arguments
	ops2 := map[string]func(string, string) error{
		"rename": syscall.Rename,
		"link":   syscall.Link,
	}

	type opStats struct {
		ok   *int64
		fail *int64
		hung *int64
	}
	stats := make(map[string]opStats)

	// pathN() returns something like /var/tmp/TestFsstress/TestFsstress.4
	pathN := func(n int) string {
		return fmt.Sprintf("%s/%s.%d", tc.mntDir, t.Name(), n)
	}

	opLoop := func(k string, n int) {
		defer wg.Done()
		op := ops1[k]
		for {
			p := pathN(1)
			atomic.AddInt64(stats[k].hung, 1)
			err := op(p)
			atomic.AddInt64(stats[k].hung, -1)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	op2Loop := func(k string, n int) {
		defer wg.Done()
		op := ops2[k]
		n2 := (n + 1) % concurrency
		for {
			p1 := pathN(n)
			p2 := pathN(n2)
			atomic.AddInt64(stats[k].hung, 1)
			err := op(p1, p2)
			atomic.AddInt64(stats[k].hung, -1)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	readdirLoop := func(k string) {
		defer wg.Done()
		for {
			atomic.AddInt64(stats[k].hung, 1)
			f, err := os.Open(tc.mntDir)
			if err != nil {
				panic(err)
			}
			_, err = f.Readdir(0)
			if err != nil {
				atomic.AddInt64(stats[k].fail, 1)
			} else {
				atomic.AddInt64(stats[k].ok, 1)
			}
			f.Close()
			atomic.AddInt64(stats[k].hung, -1)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	// prepare stats map
	var allOps []string
	for k := range ops1 {
		allOps = append(allOps, k)
	}
	for k := range ops2 {
		allOps = append(allOps, k)
	}
	allOps = append(allOps, "readdir")
	for _, k := range allOps {
		var i1, i2, i3 int64
		stats[k] = opStats{ok: &i1, fail: &i2, hung: &i3}
	}

	// spawn worker goroutines
	for i := 0; i < concurrency; i++ {
		for k := range ops1 {
			wg.Add(1)
			go opLoop(k, i)
		}
		for k := range ops2 {
			wg.Add(1)
			go op2Loop(k, i)
		}
	}
	{
		k := "readdir"
		wg.Add(1)
		go readdirLoop(k)
	}

	// spawn ls loop
	//
	// An external "ls" loop has a destructive effect that I am unable to
	// reproduce through in-process operations.
	if strings.ContainsAny(tc.mntDir, "'\\") {
		// But let's not enable shell injection.
		log.Panicf("shell injection attempt? mntDir=%q", tc.mntDir)
	}
	// --color=always enables xattr lookups for extra stress
	cmd := exec.Command("bash", "-c", "while true ; do ls -l --color=always '"+tc.mntDir+"'; done")
	err := cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() {
		cmd.Wait()
		wg.Done()
	}()

	defer cmd.Process.Kill()

	// Run the test for 1 second. If it deadlocks, it usually does within 20ms.
	time.Sleep(1 * time.Second)

	cancel()
	cmd.Process.Kill()

	// waitTimeout waits for the waitgroup for the specified max timeout.
	// Returns true if waiting timed out.
	waitTimeout := func(wg *sync.WaitGroup, timeout time.Duration) bool {
		c := make(chan struct{})
		go func() {
			defer close(c)
			wg.Wait()
		}()
		select {
		case <-c:
			return false // completed normally
		case <-time.After(timeout):
			return true // timed out
		}
	}

	if waitTimeout(&wg, time.Second) {
		t.Errorf("timeout waiting for goroutines to exit (deadlocked?)")
	}

	// Print operation statistics
	var keys []string
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("Operation statistics:")
	for _, k := range keys {
		v := stats[k]
		t.Logf("%10s: %5d ok, %6d fail, %2d hung", k, *v.ok, *v.fail, *v.hung)
	}
}

// TestStaleHardlinks creates a lot of hard links and deletes them again
// behind the back of the loopback fs. Then opens the original file.
//
// Fails at the moment. Core of the problem:
//
// 18:41:50.796468 rx 136: LOOKUP n1 ["link0"] 6b
// 18:41:50.796489 tx 136:     OK, {n2 g1 tE=0s tA=0s {M0100600 SZ=0 L=1 1026:1026 B0*4096 i0:269663 A 1616348510.793212 M 1616348510.793212 C 1616348510.795212}}
// 18:41:50.796535 rx 138: OPEN n2 {O_RDONLY,0x8000}
// 18:41:50.796557 tx 138:     2=no such file or directory, {Fh 0 }
func TestStaleHardlinks(t *testing.T) {
	// Disable all caches we can disable
	tc := newTestCase(t, &testOptions{attrCache: false, entryCache: false})

	// gvfsd-trash sets an inotify watch on mntDir and stat()s every file that is
	// created, racing with the test logic ( https://github.com/hanwen/go-fuse/issues/478 ).
	// Use a subdir to prevent that.
	if err := os.Mkdir(tc.mntDir+"/x", 0755); err != nil {
		t.Fatal(err)
	}

	// "link0" is original file
	link0 := tc.mntDir + "/x/link0"
	if fd, err := unix.Open(link0, unix.O_CREAT, 0600); err != nil {
		t.Fatal(err)
	} else {
		syscall.Close(fd)
	}
	// Create hardlinks via mntDir
	t.Logf("create link1...20, pid=%d", os.Getpid())
	for i := 1; i < 20; i++ {
		linki := fmt.Sprintf(tc.mntDir+"/x/link%d", i)
		if err := syscall.Link(link0, linki); err != nil {
			t.Fatal(err)
		}
	}
	// Delete hardlinks via origDir (behind loopback fs's back)
	t.Log("delete link1...20 behind loopback's back")
	for i := 1; i < 20; i++ {
		linki := fmt.Sprintf(tc.origDir+"/x/link%d", i)
		if err := syscall.Unlink(linki); err != nil {
			t.Fatal(err)
		}
	}
	// Try to open link0 via mntDir
	t.Log("open link0")
	fd, err := syscall.Open(link0, syscall.O_RDONLY, 0)
	if err != nil {
		t.Error(err)
	} else {
		syscall.Close(fd)
	}

}

func init() {
	syscall.Umask(0)
}

func testMountDir(dir string) error {
	opts := &Options{}
	opts.Debug = testutil.VerboseTest()
	server, err := Mount(dir, &Inode{}, opts)
	if err != nil {
		return err
	}

	server.Unmount()
	server.Wait()
	return nil
}

func TestParallelMount(t *testing.T) {
	before := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(before)
	// Per default, only 1000 FUSE mounts are allowed, then you get
	// > /usr/bin/fusermount3: too many FUSE filesystems mounted; mount_max=N can be set in /etc/fuse.conf
	// Let's stay well below 1000.
	N := 100
	todo := make(chan string, N)
	result := make(chan error, N)
	for i := 0; i < N; i++ {
		todo <- t.TempDir()
	}
	close(todo)

	P := 2
	for i := 0; i < P; i++ {
		go func() {
			for d := range todo {
				result <- testMountDir(d)
			}
		}()
	}

	for i := 0; i < N; i++ {
		e := <-result
		if e != nil {
			t.Error(e)
		}
	}
}

type handleLessCreateNode struct {
	Inode
}

var _ = (NodeCreater)((*handleLessCreateNode)(nil))

func (n *handleLessCreateNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *Inode, fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	f := &MemRegularFile{
		Attr: fuse.Attr{
			Mode: mode,
		},
	}
	ch := n.NewPersistentInode(ctx, f, StableAttr{Mode: fuse.S_IFREG})
	n.AddChild(name, ch, true)
	return ch, nil, fuse.FOPEN_KEEP_CACHE, 0
}

func TestHandleLessCreate(t *testing.T) {
	hlcn := &handleLessCreateNode{}
	dir, _ := testMount(t, hlcn, nil)

	posixtest.FileBasic(t, dir)
}

func lstatxPath(p string) (*unix.Statx_t, error) {
	var r unix.Statx_t
	err := unix.Statx(unix.AT_FDCWD, p, unix.AT_SYMLINK_NOFOLLOW,
		(unix.STATX_BASIC_STATS | unix.STATX_BTIME |
			unix.STATX_MNT_ID), // unix.STATX_DIOALIGN
		&r)
	return &r, err
}

func clearStatx(st *unix.Statx_t, mask uint32) {
	st.Mask = mask
	if mask&(unix.STATX_TYPE|unix.STATX_MODE) == 0 {
		st.Mode = 0
	}
	if mask&(unix.STATX_NLINK) == 0 {
		st.Nlink = 0
	}
	if mask&(unix.STATX_INO) == 0 {
		st.Ino = 0
	}
	if mask&(unix.STATX_ATIME) == 0 {
		st.Atime = unix.StatxTimestamp{}
	}
	if mask&(unix.STATX_BTIME) == 0 {
		st.Btime = unix.StatxTimestamp{}
	}
	if mask&(unix.STATX_CTIME) == 0 {
		st.Ctime = unix.StatxTimestamp{}
	}
	if mask&(unix.STATX_MTIME) == 0 {
		st.Mtime = unix.StatxTimestamp{}
	}

	st.Dev_minor = 0
	st.Dev_major = 0
	st.Mnt_id = 0
	st.Attributes_mask = 0
}

func TestStatx(t *testing.T) {
	tc := newTestCase(t, &testOptions{attrCache: false, entryCache: false})
	if err := os.WriteFile(tc.origDir+"/file", []byte("blabla"), 0644); err != nil {
		t.Fatal(err)
	}

	oFile := tc.origDir + "/file"
	want, err := lstatxPath(oFile)
	if err != nil {
		t.Fatal(err)
	}
	mFile := tc.mntDir + "/file"
	got, err := lstatxPath(mFile)
	if err != nil {
		t.Fatal(err)
	}
	mask := got.Mask & want.Mask
	clearStatx(got, mask)
	clearStatx(want, mask)
	if diff := pretty.Compare(got, want); diff != "" {
		t.Errorf("got, want: %s", diff)
	}

	// the following works, but does not set Fh in StatxIn
	oFD, err := os.Open(oFile)
	if err != nil {
		t.Fatal(err)
	}
	defer oFD.Close()
	mFD, err := os.Open(mFile)
	if err != nil {
		t.Fatal(err)
	}
	defer mFD.Close()

	var osx, msx unix.Statx_t
	if err := unix.Statx(int(oFD.Fd()), "", unix.AT_EMPTY_PATH,
		(unix.STATX_BASIC_STATS | unix.STATX_BTIME |
			unix.STATX_MNT_ID), // unix.STATX_DIOALIGN
		&osx); err != nil {
		t.Fatal(err)
	}
	if err := unix.Statx(int(mFD.Fd()), "", unix.AT_EMPTY_PATH,
		(unix.STATX_BASIC_STATS | unix.STATX_BTIME |
			unix.STATX_MNT_ID), // unix.STATX_DIOALIGN
		&msx); err != nil {
		t.Fatal(err)
	}

	mask = msx.Mask & osx.Mask
	clearStatx(&msx, mask)
	clearStatx(&osx, mask)
	if diff := pretty.Compare(msx, osx); diff != "" {
		t.Errorf("got, want: %s", diff)
	}
}
