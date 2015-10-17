package ftp

/*

Methods for parsing FTP data.

	Currently, this file contains functions for parsing FTP ``LIST`` command
output from a variety of FTP servers. In the future, this file may be
extended to handle other FTP parsing chores. (Or not.)

The FTP ``LIST`` parsing logic was adapted for Golang from D. J. Bernstein's
``ftpparse.c`` library. See http://cr.yp.to/ftpparse.html. The logic in this
module is functionally similar to Bernstein's parser.

Currently covered formats:

    - `EPLF`_
- UNIX *ls*, with or without group ID
    - Microsoft FTP Service
    - Windows NT FTP Server
    - VMS
    - WFTPD
- NetPresenz (Mac)
    - NetWare
    - MSDOS

.. _EPLF: http://cr.yp.to/ftp/list/eplf.html

Definitely not covered:

- Long VMS filenames, with information split across two lines.
- NCSA Telnet FTP server. Has LIST = NLST (and bad NLST for directories).

*/

import (
	"time"
	"strings"
	"strconv"
)

/*
-----------------------------------------------------------
Constants
-----------------------------------------------------------
*/
	
var MONTHS []string = []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}

type MTIME_TYPE int
const (
	UNKNOWN_MTIME_TYPE MTIME_TYPE = iota
	LOCAL_MTIME_TYPE
	REMOTE_MINUTE_MTIME_TYPE
	REMOTE_DAY_MTIME_TYPE
)
/*
 MTIME_TYPE identifies how a modification time ought to be interpreted
 (assuming the caller cares).

- LOCAL: Time is local to the client, granular to (at least) the minute
- REMOTE_MINUTE: Time is local to the server and granular to the minute
- REMOTE_DAY: Time is local to the server and granular to the day.
- UNKNOWN: Time's locale is unknown.
*/

type ID_TYPE int
const (
	UNKNOWN_ID_TYPE ID_TYPE = iota
	FULL_ID_TYPE
)
/*
ID_TYPE identifies how a file's identifier should be interpreted.

- FULL: The ID is known to be complete.
- UNKNOWN: The ID is not set or its type is unknown.
*/

/*
-----------------------------------------------------------
Globals
-----------------------------------------------------------
*/
var now = time.Now()
var currentYear = now.Year()


/*
 ParseLine() function returns an
instance of this struct, capturing the parsed data.

:IVariables:

name : str
The name of the file, if parsable

try_cwd : bool
``true`` if the entry might be a directory (i.e., the caller
might want to try an FTP ``CWD`` command), ``false`` if it
            cannot possibly be a directory.

try_retr : bool
``true`` if the entry might be a retrievable file (i.e., the caller
might want to try an FTP ``RETR`` command), ``false`` if it
            cannot possibly be a file.

size : long
The file's size, in bytes

mtime : Time
The file's modification time.

mtime_type : `MTIME_TYPE`
            How to interpret the modification time. See `MTIME_TYPE`.

id : str
            A unique identifier for the file. The unique identifier is unique
on the *server*. On a Unix system, this identifier might be the
device number and the file's inode; on other system's, it might
            be something else. It's also possible for this field to be ``nil``.

id_type : `ID_TYPE`

link_dest :  Link destination when listing is a link

*/
type FTPListData struct {

	RawLine string
	Name string
	TryCwd bool
	TryRetr bool
	Size uint64
	MtimeType MTIME_TYPE
	Mtime time.Time
	IdType ID_TYPE
	Id string
	LinkDest string
}

func newFTPListData(rawLine string) (fdata *FTPListData) {
	fdata = new(FTPListData)
	fdata.RawLine = rawLine
	fdata.Name = ""
	fdata.TryCwd = false
	fdata.TryRetr = false
	fdata.MtimeType = UNKNOWN_MTIME_TYPE
	fdata.IdType = UNKNOWN_ID_TYPE
	fdata.Id = ""
	fdata.LinkDest = ""
	return
}

func ParseLine(ftpListLine string) (fdata *FTPListData) {
	now = time.Now()
	currentYear = now.Year()

	buf := ftpListLine
	if len(buf) < 2 {
		//an empty name in EPLF, with no info, could be 2 chars
		return nil
	}
	c := byte(buf[0])
	switch c {
	case '+':
		return parseEPLF(buf)
	case 'b', 'c', 'd', 'l', 'p', 's', '-':
		return parseUNIXStyle(buf)
		
	}
	if index := strings.Index(buf, ";"); index > 0 {
		return parseMultinet(buf, index)
	}
	if c >= '0' && c <= '9' {
		return parseMSDOS(buf)
	}
	return nil
}


func parseEPLF(buf string) (fdata *FTPListData) {
	/*
	  see http://cr.yp.to/ftp/list/eplf.html
	  "+i8388621.29609,m824255902,/,\tdev"
	  "+i8388621.44468,m839956783,r,s10376,\tRFCEPLF"
	*/
	fdata = newFTPListData(buf)
	buf = strings.Trim(buf, "\t\n\r ")
	i := 1
	for j:=1 ; j<len(buf); j++ {
		if buf[j] == '\t' {
			fdata.Name = buf[j+1:]
			break
		}
		if buf[j] == ',' {
			c := buf[i]
			switch c {
			case '/':
				fdata.TryCwd = true
			case 'r':
				fdata.TryRetr = true
			case 's':
				size, err := strconv.ParseUint(buf[i+1:j], 10, 64)
				if err != nil { return nil }
				fdata.Size = size
			case 'm':
				fdata.MtimeType = LOCAL_MTIME_TYPE
				unixtime, err := strconv.ParseInt(buf[i+1:j], 10, 64)
				if err != nil { return nil }
				fdata.Mtime = time.Unix(unixtime, 0)
			case 'i':
				fdata.IdType = FULL_ID_TYPE
				fdata.Id = buf[i+1:j-i-1]
				
			}
			i = j+1
		}
		
	}
	return
}

/*

    UNIX ls does not show the year for dates in the last six months.
    So we have to guess the year.
    
    Apparently NetWare uses ``twelve months'' instead of ``six months''; ugh.
    Some versions of ls also fail to show the year for future dates.

*/

func guessTime(month time.Month, mday, hour, minute int) (t int64) {
	
	year := 0 
	t = 0
	ul := currentYear + 100
	for year = currentYear - 1 ; year < ul ; year ++ {
		t = getMtime(year, month, mday, hour, minute, 0)
		if (now.Unix() - t) < (350 * 86400) {
			return t
		}
	}
	return 0
	
}

func getMtime(year int, month time.Month, mday, hour, minute, second int) (t int64) {
	l, _ := time.LoadLocation("UTC")
	return time.Date(year, month, mday, hour, minute, second, 0, l).Unix()
}

func getMonth(buf string) (m time.Month) {
	if len(buf) == 3 {
		for i:=0 ; i<12 ; i++ {
			if strings.ToLower(buf) == MONTHS[i] {
				return time.Month(i+1)
			}
		}
	}
	return time.Month(-1)
}

func parseInt(num string) (n int) {
	x, _ := strconv.ParseInt(num, 10, 32)
	return int(x)
}

func parseUNIXStyle(buf string) (fdata *FTPListData) {
	/*
	
	 UNIX-style listing, without inum and without blocks:
	 "-rw-r--r--   1 root     other        531 Jan 29 03:26 README"
	 "dr-xr-xr-x   2 root     other        512 Apr  8  1994 etc"
	 "dr-xr-xr-x   2 root     512 Apr  8  1994 etc"
	 "lrwxrwxrwx   1 root     other          7 Jan 25 00:17 bin -> usr/bin"
	
	 Also produced by Microsoft's FTP servers for Windows:
	 "----------   1 owner    group         1803128 Jul 10 10:18 ls-lR.Z"
	 "d---------   1 owner    group               0 May  9 19:45 Softlib"
	
	 Also WFTPD for MSDOS:
	  "-rwxrwxrwx   1 noone    nogroup      322 Aug 19  1996 message.ftp"
	
	Also NetWare:
	"d [R----F--] supervisor            512       Jan 16 18:53    login"
	"- [R----F--] rhesus             214059       Oct 20 15:27    cx.exe"
        
	Also NetPresenz for the Mac:
        "-------r--         326  1391972  1392298 Nov 22  1995 MegaPhone.sit"
        "drwxrwxr-x               folder        2 May 10  1996 network"
	
	*/

	fdata = newFTPListData(buf)
	buf = strings.Trim(buf, "\t\n\r ")
	buflen := len(buf)
	c := buf[0]
	switch c {
	case 'd':
		fdata.TryCwd = true
	case '-':
		fdata.TryRetr = true
	case 'l':
		fdata.TryRetr = true
		fdata.TryCwd = true
	}

	var size uint64 = 0
	var month time.Month = 1
	var mday int = 0
	var hour int = 0
	var minute int = 0
	var year int = 0
	state := 1
	i := 0
	//tokens := strings.Fields(buf)
	for j:=1 ; j<buflen ; j++ {

		if (buf[j] == ' ') && (buf[j-1] != ' ') {
			if state == 1 { // skipping perm
				state = 2
			} else if state == 2 { //skipping nlink
				state = 3
				if (j-i) == 6 && (buf[i] == 'f') { // Netpresenz
					state = 4
				}
			} else if state == 3 { // skipping UID/GID
				state = 4
			} else if state == 4 { // getting tentative size
				size, _ = strconv.ParseUint(buf[i:j], 10, 64)
				state = 5
			} else if state == 5 { // searching for month, else getting tentative size
				month = getMonth(buf[i:j])
				if month >= 0 {
					state = 6
				} else {
					size, _ = strconv.ParseUint(buf[i:j], 10, 64)
				}
			} else if state == 6 { // have size and month
				mday = parseInt(buf[i:j])
				state = 7
			} else if state == 7 { // have size, month, mday
				if ((j - i) == 4) && (buf[i+1] == ':') {
					hour = parseInt(string(buf[i]))
					minute = parseInt(buf[i+2:i+4])
					fdata.MtimeType = REMOTE_MINUTE_MTIME_TYPE
					fdata.Mtime = time.Unix(guessTime(month, mday, hour, minute), 0)
				} else if (j - i == 5) && (buf[i+2] == ':') {
					hour = parseInt(buf[i:i+2])
					minute = parseInt(buf[i+3:i+5])
					fdata.MtimeType = REMOTE_MINUTE_MTIME_TYPE
					fdata.Mtime = time.Unix(guessTime(month, mday, hour, minute), 0)
				} else if (j - i) >= 4 {
					year = parseInt(buf[i:j])
					fdata.MtimeType = REMOTE_DAY_MTIME_TYPE
					fdata.Mtime = time.Unix(getMtime(year, month, mday, 0, 0, 0), 0)
				} else {
					break
				}
				fdata.Name = buf[j+1 : ]
				state = 8
			} else if state == 8 { // twiddling thumbs
				// pass
			}
			
			for i = j + 1 ; (i < buflen) && (buf[i] == ' ') ; i++  {
			}
			
		}
	
	}
	fdata.Size = size
	if c == 'l' {
		for i=0 ; (i + 3) < len(fdata.Name) ; i++ {
			if fdata.Name[i:i+4] == " -> " {
				tmp := fdata.Name
				fdata.Name = tmp[:i]
				fdata.LinkDest = tmp[i+4:]
				break
			}
		}
	}

	// eliminate extra NetWare spaces
	if (buf[1] == ' ') || (buf[1] == '[') {
		namelen := len(fdata.Name)
		if namelen > 3 {
			fdata.Name = strings.TrimSpace(fdata.Name)
		}
			
	}

	return
}

func indexAfter(s, sep string, i int) int {
	x := i + strings.Index(s[i:], sep)
	if x < i {
		return -1
	}
	return x
}

func skip(s string, i int, c byte) int {
	for s[i] == c {
		i += 1
		if i == len(s) {
			return -1
		}
	}
	return i
}

func parseMultinet(buf string, i int) (fdata *FTPListData) {

	/*

	MultiNet (some spaces removed from examples)
	"00README.TXT;1      2 30-DEC-1996 17:44 [SYSTEM] (RWED,RWED,RE,RE)"
	"CORE.DIR;1          1  8-SEP-1996 16:09 [SYSTEM] (RWE,RWE,RE,RE)"

	and non-MultiNet VMS:
	"CII-MANUAL.TEX;1  213/216  29-JAN-1996 03:33:12  [ANONYMOU,ANONYMOUS]   (RWED,RWED,,)"
	
	*/
		
	fdata = newFTPListData(buf)
	buf = strings.Trim(buf, "\t\n\r ")
	fdata.Name = buf[:i]
	buflen := len(buf)

	var month time.Month = 1
	var mday int = 0
	var hour int = 0
	var minute int = 0
	var year int = 0

	if i > 4 {
		if buf[i-4:i] == ".DIR" {
			l := len(fdata.Name)
			fdata.Name = fdata.Name[0:l-4]
			fdata.TryCwd = true
		}
	}

	if fdata.TryCwd == false {
		fdata.TryRetr = true
	}

	for p:=0 ; p < 2 ; p++ {
		
		if i = indexAfter(buf, " ", i); i == -1 {
			return
		}

		if i = skip(buf, i, ' '); i == -1 {
			return
		}
	}
	j := i
	if j = indexAfter(buf, "-", j); j == -1 {
		return
	}
	mday = parseInt(buf[i:j])

	if j = skip(buf, j, '-'); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, "-", j); j == -1 {
		return
	}
	
	if month = getMonth(buf[i:j]); month < 0 {
		return
	}
	if j = skip(buf, j, '-'); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, " ", j); j == -1 {
		return
	}
	year = parseInt(buf[i:j])
	if j = skip(buf, j, ' '); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, ":", j); j == -1 {
		return
	}
	hour = parseInt(buf[i:j])
	if j = skip(buf, j, ':'); j == -1 {
		return
	}
	i = j
	for (buf[j] != ':') && (buf[j] != ' ') {
		j += 1
		if j == buflen {
			return
		}
	}
	minute = parseInt(buf[i:j])
	
	fdata.MtimeType = REMOTE_MINUTE_MTIME_TYPE
	fdata.Mtime = time.Unix(getMtime(year, month, mday, hour, minute, 0), 0)
	return
	
}

func parseMSDOS(buf string) (fdata *FTPListData) {

	/*

	MSDOS format
	04-27-00  09:09PM       <DIR>          licensed
	07-18-00  10:16AM       <DIR>          pub
	04-14-00  03:47PM                  589 readme.htm

	*/

	fdata = newFTPListData(buf)
	buf = strings.Trim(buf, "\t\n\r ")

	var month time.Month = 1
	var mday int = 0
	var hour int = 0
	var minute int = 0
	var year int = 0

	buflen := len(buf)
	i := 0
	j := 0

	if j = indexAfter(buf, "-", j); j == -1 {
		return
	}
	month = time.Month(parseInt(buf[i:j]))

	if j = skip(buf, j, '-'); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, "-", j); j == -1 {
		return
	}
	mday = parseInt(buf[i:j])

	if j = skip(buf, j, '-'); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, " ", j); j == -1 {
		return
	}
	year = parseInt(buf[i:j])
	if year < 50 {
		year += 2000
	}
	if year < 1000 {
		year += 1900
	}

	if j = skip(buf, j, ' '); j == -1 {
		return
	}
	i = j
	if j = indexAfter(buf, ":", j); j == -1 {
		return
	}
	hour = parseInt(buf[i:j])
	if j = skip(buf, j, ':'); j == -1 {
		return
	}
	i = j
	for (buf[j] != 'A') && (buf[j] != 'P') {
		j += 1
		if j == buflen {
			return
		}
	}

	minute = parseInt(buf[i:j])

	if buf[j] == 'A' {
		j += 1
		if j == buflen {
			return
		}
	}
	if buf[j] == 'P' {
		hour = (hour + 12) % 24
		j += 1
		if j == buflen {
			return
		}
	}
	if buf[j] == 'M' {
		j += 1
		if j == buflen {
			return
		}
	}
	if j = skip(buf, j, ' '); j == -1 {
		return
	}
	if buf[j] == '<' {
		fdata.TryCwd = true
		if j = indexAfter(buf, " ", j); j == -1 {
			return
		}
	} else {
		i = j
		if j = indexAfter(buf, " ", j); j == -1 {
			return
		}
		fdata.Size, _ = strconv.ParseUint(buf[i:j], 10, 64)
		fdata.TryRetr = true
	}

	if j = skip(buf, j, ' '); j == -1 {
		return
	}
	fdata.Name = buf[j:]
	fdata.MtimeType = REMOTE_MINUTE_MTIME_TYPE
	fdata.Mtime = time.Unix(getMtime(year, month, mday, hour, minute, 0), 0)
	return
}


