package ftp

import (
    "bufio"
    "errors"
    "fmt"
    "io"
    "net"
    "net/textproto"
    "strconv"
    "strings"
	"time"
	"log"
)

type ServerConn struct {
    conn *textproto.Conn
    host string
	realhost string
	timeout_reads bool
	timeout_duration time.Duration
}

type response struct {
    conn net.Conn
    c    *ServerConn
}

// Connect to a ftp server and returns a ServerConn handler.
func Connect(addr string, timeout_duration time.Duration) (*ServerConn, error) {
    if strings.Contains(addr, ":") == false {
		addr = addr + ":21"
    }
    netconn, err := net.DialTimeout("tcp", addr, timeout_duration)
    if err != nil {
		return nil, err
    }

	conn := textproto.NewConn(netconn)

    a := strings.SplitN(addr, ":", 2)
    c := &ServerConn{conn: conn, host: a[0], realhost: "", timeout_reads: false, timeout_duration: timeout_duration}

    _, _, err = c.conn.ReadCodeLine(StatusReady)
    if err != nil {
		c.Quit()
		return nil, err
    }

    return c, nil
}

func (c *ServerConn) Login(user, password string) error {
	parts := strings.SplitN(user, "@", 2)
	if len(parts) == 2 {
		c.realhost = parts[1]
	}

	log.Println(c.realhost, "Entered goftp.Login")
	defer 	log.Println(c.realhost, "Leaving goftp.Login")
    _, _, err := c.cmd(StatusUserOK, "USER %s", user)
    if err != nil {
		return err
    }

    code, _, err := c.cmd(StatusLoggedIn, "PASS %s", password)
    if code == StatusLoggedIn {
		return nil
    }
    return err
}

// Enter passive mode
func (c *ServerConn) pasv() (port int, err error) {
    c.conn.Cmd("PASV")
    code, line, err := c.conn.ReadCodeLine(StatusExtendedPassiveMode)
    if (err != nil) && (code != StatusPassiveMode) {
		return
    } else {
		err = nil
    }
    start, end := strings.Index(line, "("), strings.Index(line, ")")
    if start == -1 || end == -1 {
		err = errors.New("Invalid PASV response format")
		return
    }
    s := strings.Split(line[start+1:end], ",")
    l1, _ := strconv.Atoi(s[len(s)-2])
    l2, _ := strconv.Atoi(s[len(s)-1])
    port = l1*256 + l2
    return
}

// Enter extended passive mode
func (c *ServerConn) epsv() (port int, err error) {
    c.conn.Cmd("EPSV")
    _, line, err := c.conn.ReadCodeLine(StatusExtendedPassiveMode)
    if err != nil {
		return
    }
    start := strings.Index(line, "|||")
    end := strings.LastIndex(line, "|")
    if start == -1 || end == -1 {
		err = errors.New("Invalid EPSV response format")
		return
    }
    port, err = strconv.Atoi(line[start+3 : end])
    return
}

// Open a new data connection using passive mode
func (c *ServerConn) openDataConn() (net.Conn, error) {
    port, err := c.pasv()
    port, err = c.pasv()
    port, err = c.pasv()
    if err != nil {
		//fmt.Println( "c.pasv() err=",err)
		return nil, err
    }

    //Build the new net address string
    addr := fmt.Sprintf("%s:%d", c.host, port)
    //fmt.Println("New Address : host :", c.host, "port :", port)
    tcpAddr, _ := net.ResolveTCPAddr("tcp4", addr)
    conn, err := net.DialTCP("tcp", nil, tcpAddr)
    if err != nil {
    	log.Println(c.realhost, "openDataConn : DialTCP returned err=",err)
    	return nil, err
    }
    // addr := fmt.Sprintf("%s:%d", c.host, port)
    // conn, err := net.DialTimeout("tcp", addr, time.Duration(2400)*time.Second)
    // if err != nil {
    // 	return nil, err
    // }

    return conn, nil
}

// Helper function to execute a command and check for the expected code
func (c *ServerConn) cmd(expected int, format string, args ...interface{}) (int, string, error) {
    _, err := c.conn.Cmd(format, args...)
    if err != nil {
		return 0, "", err
    }
    code, line, err := c.conn.ReadCodeLine(expected)
    iter := 0
    for (code != expected) && (code < 400) && (iter <10) {
		//repeat till expected code comes up. but quit if negative code comes up
		code, line, err = c.conn.ReadCodeLine(expected)
		iter = iter + 1
    }
    // for code == StatusLoggedIn && expected == StatusPathCreated {
    // 	code, line, err = c.conn.ReadCodeLine(expected)
    // }
    // for code == StatusRequestedFileActionOK && expected == StatusPathCreated {
    // 	code, line, err = c.conn.ReadCodeLine(expected)
    // }
    return code, line, err
}

// Helper function to execute commands which require a data connection
func (c *ServerConn) cmdDataConn(format string, args ...interface{}) (net.Conn, error) {
    conn, err := c.openDataConn()
    if err != nil {
		return nil, err
    }
    
    _, err = c.conn.Cmd(format, args...)
    if err != nil {
		log.Println(c.realhost, "cmdDataconn : c.conn.Cmd returned err =", err)
		conn.Close()
		return nil, err
    }

    code, msg, err := c.conn.ReadCodeLine(-1)
    if (err != nil) &&  code != StatusAlreadyOpen && code != StatusAboutToSend && code != StatusPassiveMode {
		log.Println(c.realhost, "cmdDataConn : c.conn.ReadCodeLine(-1) returned code=",code, "msg=",msg, "err=",err)
		conn.Close()
		return nil, err
    }
    if code != StatusAlreadyOpen && code != StatusAboutToSend && code != StatusPassiveMode {
		log.Println(c.realhost, "cmdDataConn : c.conn.ReadCodeLine(-1) returned : code=",code, "msg=",msg, "err=",err)
		if code != StatusCanNotOpenDataConnection {
			conn.Close()
		}
		return nil, &textproto.Error{code, msg}
    }

    return conn, nil
}

func (c *ServerConn) SetReadTimeoutFlag() {
	c.timeout_reads = true
}

func (c *ServerConn) UnsetReadTimeoutFlag() {
	c.timeout_reads = false
}

func (c *ServerConn) List(path string) (entries []*FTPListData, err error) {
	log.Println(c.realhost, "Entered goftp.List")
	defer 	log.Println(c.realhost, "Leaving goftp.List")
	c.SetReadTimeoutFlag()
	defer c.UnsetReadTimeoutFlag()
	log.Println(c.realhost, "List : about to call c.cmdDataConn")
    conn, err := c.cmdDataConn("LIST %s", path)
	log.Println(c.realhost, "List : finished calling c.cmdDataConn")
    //fmt.Println("List : err = ", err)
    if err != nil {
		log.Println(c.realhost, "List : c.cmdDataConn returned err = ", err)
		return
    }

    r := &response{conn, c}
    defer r.Close()

	log.Println(c.realhost, "List : About to call bufio.NewReader(r)")
    bio := bufio.NewReader(r)
	log.Println(c.realhost, "List : Finished calling bufio.NewReader(r); About to enter for loop with bio.ReadString('\\n')")
	
    for {
		line, e := bio.ReadString('\n')
		log.Println(c.realhost, "List : RALFB.RS")
		if e == io.EOF {
			log.Println(c.realhost, "List : bio.ReadString('\\n') returned err == io.EOF")
			break
		} else if e != nil {
			log.Println(c.realhost, "List : bio.ReadString('\\n') returned err =", e, "and about to return from List()")
			return nil, e
		}
		ftplistdata := ParseLine(line)
		log.Println(c.realhost, "List : PAL")
		entries = append(entries, ftplistdata)
    }
	log.Println(c.realhost, "List : about to return normally")
    return
}

// Changes the current directory to the specified path.
func (c *ServerConn) ChangeDir(path string) error {
	log.Println(c.realhost, "Entered goftp.ChangeDir")
	defer 	log.Println(c.realhost, "Leaving goftp.ChangeDir")
    code, _, err := c.cmd(StatusRequestedFileActionOK, "CWD %s", path)
    //fmt.Println(code)
    if code == StatusClosingDataConnection {
		return nil
    }
    return err
}

// Changes the current directory to the parent directory.
// ChangeDir("..")
func (c *ServerConn) ChangeDirToParent() error {
	log.Println(c.realhost, "Entered goftp.ChangeDirToParent")
	defer 	log.Println(c.realhost, "Leaving goftp.ChangeDirToParent")
    _, _, err := c.cmd(StatusRequestedFileActionOK, "CDUP")
    return err
}

// Returns the path of the current directory.
func (c *ServerConn) CurrentDir() (string, error) {
	log.Println(c.realhost, "Entered goftp.CurrentDir")
	defer 	log.Println(c.realhost, "Leaving goftp.CurrentDir")
    _, msg, err := c.cmd(StatusPathCreated, "PWD")
    //fmt.Println("PWD err : ", err, "msg : ", msg, "code :", code)
    if err != nil {
		//fmt.Println("PWD err : ", err, "msg : ", msg, "code :", code)
		return "", err
    }
    //fmt.Println("PWD success")
    start := strings.Index(msg, "\"")
    end := strings.LastIndex(msg, "\"")

    if start == -1 || end == -1 {
		return "", errors.New("Unsuported PWD response format")
    }

    return msg[start+1 : end], nil
}

// Retrieves a file from the remote FTP server.
// The ReadCloser must be closed at the end of the operation.
func (c *ServerConn) Retr(path string) (io.ReadCloser, error) {
	log.Println(c.realhost, "Entered goftp.Retr")
	defer log.Println(c.realhost, "Leaving goftp.Retr")
    _, _, err := c.cmd(StatusCommandOK, "TYPE I")
    log.Println(c.realhost, "Retr : Set type to binary : err= ", err, "About to send RETR")
    conn, err := c.cmdDataConn("RETR %s", path)
    if err != nil {
		log.Println(c.realhost, "Retr : cmdDataConn returned err=", err)
		return nil, err
    }

	log.Println(c.realhost, "Retr : Successfully sent RETR, returning ReadCloser")
    r := &response{conn, c}
    return r, nil
}

// Uploads a file to the remote FTP server.
// This function gets the data from the io.Reader. Hint: io.Pipe()
func (c *ServerConn) Stor(path string, r io.Reader) error {
    conn, err := c.cmdDataConn("STOR %s", path)
    if err != nil {
		return err
    }

    _, err = io.Copy(conn, r)
    conn.Close()
    if err != nil {
		return err
    }

    _, _, err = c.conn.ReadCodeLine(StatusClosingDataConnection)
    return err
}

// Renames a file on the remote FTP server.
func (c *ServerConn) Rename(from, to string) error {
    _, _, err := c.cmd(StatusRequestFilePending, "RNFR %s", from)
    if err != nil {
		return err
    }

    _, _, err = c.cmd(StatusRequestedFileActionOK, "RNTO %s", to)
    return err
}

// Deletes a file on the remote FTP server.
func (c *ServerConn) Delete(path string) error {
    _, _, err := c.cmd(StatusRequestedFileActionOK, "DELE %s", path)
    return err
}

// Creates a new directory on the remote FTP server.
func (c *ServerConn) MakeDir(path string) error {
    _, _, err := c.cmd(StatusPathCreated, "MKD %s", path)
    return err
}

// Removes a directory from the remote FTP server.
func (c *ServerConn) RemoveDir(path string) error {
    _, _, err := c.cmd(StatusRequestedFileActionOK, "RMD %s", path)
    return err
}

// Sends a NOOP command. Usualy used to prevent timeouts.
func (c *ServerConn) NoOp() error {
    _, _, err := c.cmd(StatusCommandOK, "NOOP")
    return err
}

// Properly close the connection from the remote FTP server.
// It notifies the remote server that we are about to close the connection,
// then it really closes it.
func (c *ServerConn) Quit() error {
	log.Println(c.realhost, "Entered goftp.Quit")
	defer log.Println(c.realhost, "Leaving goftp.Quit")
    c.conn.Cmd("QUIT")
    return c.conn.Close()
}

func (r *response) Read(buf []byte) (int, error) {
	if (r.c.timeout_reads == true) && (r.c.timeout_duration.Seconds() >= 59.0) {
		r.conn.SetReadDeadline(time.Now().Add(r.c.timeout_duration))
	}
    n, err := r.conn.Read(buf)
    //fmt.Println("Read ", n, "bytes", "err = ", err)
    if err == io.EOF {
		code, _, err2 := r.c.conn.ReadCodeLine(StatusClosingDataConnection)
		//fmt.Println("code = ", code, "err2 = ", err2 )
		if (err2 != nil) && (code != StatusPassiveMode) && (code != StatusClosingDataConnection) && (code != StatusAboutToSend) {
			err = err2
		}
    } else if err != nil {
		log.Println(r.c.realhost, "goftp : Read() failed with err =", err)
	}
    return n, err
}

func (r *response) Close() error {
    return r.conn.Close()
}
