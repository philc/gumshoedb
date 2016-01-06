package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/philc/gumshoedb/internal/golang.org/x/crypto/ssh"
	"github.com/philc/gumshoedb/internal/golang.org/x/crypto/ssh/agent"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("usage: %s HOST1 HOST2", os.Args[0])
	}
	host1 := os.Args[1]
	host2 := os.Args[2]

	conn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		log.Println("Cannot connect to ssh agent:", err)
		log.Fatalln("(Is your ssh-agent running? Is $SSH_AUTH_SOCK set?)")
	}
	ag := agent.NewClient(conn)
	config := &ssh.ClientConfig{
		User: "ubuntu",
		Auth: []ssh.AuthMethod{ssh.PublicKeysCallback(ag.Signers)},
	}
	client, err := ssh.Dial("tcp", host1+":22", config)
	if err != nil {
		log.Fatalln("Dial error:", err)
	}
	if err := agent.ForwardToAgent(client, ag); err != nil {
		log.Fatalln("ForwardToAgent error:", err)
	}
	session, err := client.NewSession()
	if err != nil {
		log.Fatalln("Session error:", err)
	}
	defer session.Close()
	session.Stdout = os.Stdout
	session.Stderr = os.Stderr
	if err := agent.RequestAgentForwarding(session); err != nil {
		log.Fatalln("Error requesting agent forwarding:", err)
	}
	sshCmd := fmt.Sprintf("ssh -o StrictHostKeyChecking=no %s 'echo ok'", host2)
	if err := session.Run(sshCmd); err != nil {
		log.Fatalln("Cannot run command:", err)
	}
}
