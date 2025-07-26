package connection

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"

	"github.com/singhpranshu/btree-db/src/runner"
)

type DBServer struct {
	Host   string
	Port   string
	runner *runner.Runner
}

func NewDBServer(host string, port string, runner *runner.Runner) *DBServer {
	return &DBServer{
		Host:   host,
		Port:   port,
		runner: runner,
	}
}
func (db *DBServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", db.Host, db.Port))
	if err != nil {
		fmt.Println("Error:", err)
		panic("failed to start server")
	}
	defer listener.Close()
	fmt.Printf("Server started on %s:%s\n", db.Host, db.Port)

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println("Client connected:", conn.RemoteAddr())

		// Handle client connection in a goroutine
		go db.handleClient(conn)
	}
}

func (db *DBServer) handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		lengthBuffer := make([]byte, 4)
		_, err := conn.Read(lengthBuffer)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Client disconnected.")
				return
			}
			fmt.Println("Error:", err)
			return
		}

		dataLength := binary.LittleEndian.Uint32(lengthBuffer)
		fmt.Println("Received length buffer:", dataLength)

		// Read the actual data
		buffer := make([]byte, dataLength)
		_, err = conn.Read(buffer)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Client disconnected.")
				return
			}
			fmt.Println("Error:", err)
			return
		}

		// Process and use the data (here, we'll just print it)
		fmt.Printf("Received: %s\n", string(buffer[0:dataLength-1]))

		isError := false

		data, err := (func() (interface{}, error) {
			defer (func() {
				r := recover()
				isError = true
				errorstr := fmt.Sprintf("Error: %v", r)
				lengthBuffer = make([]byte, 4)
				_, err := conn.Write(binary.LittleEndian.AppendUint32(lengthBuffer, uint32(len(errorstr))))
				if err != nil {
					_, err := conn.Write([]byte(fmt.Sprintf("Error: %v", r)))
					if err != nil {
						return
					}
				}
			})()
			return db.runner.Run(string(buffer[0 : dataLength-1]))
		})()
		if isError {
			fmt.Println("Error processing request, response sent to client.")
			continue
		}
		// data, err := db.runner.Run(string(buffer[0 : dataLength-1]))
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		r, err := json.Marshal(data)
		if err != nil {
			fmt.Println("Error marshalling response:", err)
			return
		}
		fmt.Println(r)
		responseLength := uint32(len(r))
		lengthBuffer = make([]byte, 4)
		binary.LittleEndian.PutUint32(lengthBuffer, responseLength)
		_, err = conn.Write(lengthBuffer)
		if err != nil {
			fmt.Println("Error sending length:", err)
			return
		}
		_, err = conn.Write(r)
		if err != nil {
			fmt.Println("Error sending response:", err)
			return
		}

		fmt.Println("Response sent to client.")
	}

}
