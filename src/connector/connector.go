package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Terminal struct {
	conn net.Conn
}

func NewTerminal(conn net.Conn) *Terminal {
	return &Terminal{
		conn: conn,
	}
}
func (t *Terminal) Connect() error {
	// Implement the logic to connect to the terminal
	reader := bufio.NewReader(os.Stdin) // Create a reader for standard input
	for {
		fmt.Println("Enter the query to send to the server")
		message, err := reader.ReadString('\n') // Read the entire line, including spaces
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		fmt.Println("Read:", len(message), "Message:", message)
		lengthBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(lengthBuffer, uint32(len(message)))
		t.conn.Write(lengthBuffer)
		t.conn.Write([]byte(message))
		resLengthBuffer := make([]byte, 4)
		_, err = t.conn.Read(resLengthBuffer)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		dataLength := binary.LittleEndian.Uint32(resLengthBuffer)
		buffer := make([]byte, dataLength)
		_, err = t.conn.Read(buffer)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println("Received response from server:", string(buffer))
	}
}

func main() {
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
	}
	defer conn.Close()
	terminal := NewTerminal(conn)
	if err := terminal.Connect(); err != nil {
		fmt.Println("Error connecting to terminal:", err)
		return
	}
	fmt.Println("Connected to terminal successfully.")
}
