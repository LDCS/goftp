package ftp

import (
	"testing"
	"time"
)

type line struct {
	line      string
	stype     string
	size      uint64
	mtime     time.Time
	name      string
	tryCwd    bool
}

var l, _ = time.LoadLocation("UTC")

var yr =  map[bool]int{ true: currentYear, false: currentYear-1 }

var listTests = []line{
	
	line{"+i9872342.32142,m1229473595,/,\tpub", "ELPF", 0, time.Date(2008, 12, 17, 0, 26, 35, 0, l), "pub", true},
	line{"+i9872342.32142,m1229473595,r,s10376,\tREADME.txt", "ELPF",
		10376, time.Date(2008, 12, 17, 0, 26, 35, 0, l), "README.txt", false},
	
	line{"-rw-r--r--   1 root     other     531 Jan 29 03:26 README", "Unix",
		531, time.Date(currentYear, 1, 29, 03, 26, 0, 0, l), "README", false},
	line{"dr-xr-xr-x   2 root     other        512 Apr  8  2003 etc", "Unix",
		512, time.Date(2003, 4, 8, 0, 0, 0, 0, l), "etc", true},
	line{"-rw-r--r--   1 1356107  15000      4356349 Nov 23 11:34 09 Ribbons Undone.wma", "Unix",
		4356349, time.Date(yr[time.Now().Month()>=11], 11, 23, 11, 34, 0, 0, l), "09 Ribbons Undone.wma", false},
	

	line{"----------   1 owner    group         1803128 Jul 10 10:18 ls-lR.Z", "Windows",
		1803128, time.Date(yr[time.Now().Month()>=7], 7, 10, 10, 18, 0, 0, l), "ls-lR.Z", false},
	line{"d---------   1 owner    group               0 May  9 19:45 foo bar", "Windows",
		0, time.Date(yr[time.Now().Month()>=5], 5, 9, 19, 45, 0, 0, l), "foo bar", true},

	line{"d [R----F--] supervisor    512    Jan 16 18:53    login", "NetWare",
		512, time.Date(yr[time.Now().Month()>=1], 1, 16, 18, 53, 0, 0, l), "login", true},

	line{"drwxrwxr-x               folder   2 May 10  1996 bar.sit", "NetPresenz",
		2, time.Date(1996, 5, 10, 0, 0, 0, 0, l), "bar.sit", true},

	line{"CORE.DIR;1      1 8-NOV-1999 07:02 [SYSTEM] (RWED,RWED,RE,RE)", "MultiNet/VMS",
		0, time.Date(1999, 11, 8, 7, 2, 0, 0, l), "CORE", true},
	line{"00README.TXT;1      2 30-DEC-1976 17:44 [SYSTEM] (RWED,RWED,RE,RE)", "MultiNet/VMS",
		0, time.Date(1976, 12, 30, 17, 44, 0, 0, l), "00README.TXT", false},
	line{"CII-MANUAL.TEX;1  213/216  29-JAN-1996 03:33:12  [ANONYMOU,ANONYMOUS]   (RWED,RWED,,)", "MultiNet/VMS",
		0, time.Date(1996, 1, 29, 03, 33, 0, 0, l), "CII-MANUAL.TEX", false}, // Doesn't parse the seconds
	
	line{"04-27-00  09:09PM       <DIR>          licensed", "MS-DOS",
		0, time.Date(2000, 4, 27, 21, 9, 0, 0, l), "licensed", true},
	line{"11-18-03  10:16AM       <DIR>          pub", "MS-DOS",
		0, time.Date(2003, 11, 18, 10, 16, 0, 0, l), "pub", true},
	line{"04-14-99  03:47PM                  589 readme.htm", "MS-DOS",
		589, time.Date(1999, 04, 14, 15, 47, 0, 0, l), "readme.htm", false},

}

func TestParseListLine(t *testing.T) {
	for _, lt := range listTests {
		entry := ParseLine(lt.line)
		if entry.Name != lt.name {
			t.Errorf("parseLine(%v).name = '%v', want '%v'. ServerType = %s", lt.line, entry.Name, lt.name, lt.stype)
		}
		if entry.TryCwd != lt.tryCwd {
			t.Errorf("parseLine(%v).tryCwd = %v, want %v. ServerType = %s", lt.line, entry.TryCwd, lt.tryCwd, lt.stype)
		}
		if entry.Size != lt.size {
			t.Errorf("parseLine(%v).size = %v, want %v. ServerType = %s", lt.line, entry.Size, lt.size, lt.stype)
		}
		if entry.Mtime.UTC().Equal(lt.mtime.UTC()) == false {
			t.Errorf("parseLine(%v).mtime = %v, want %v. ServerType = %s", lt.line, entry.Mtime.UTC(), lt.mtime.UTC(), lt.stype)
		}
	}
}
