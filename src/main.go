
// This is just a simple test program that 
// I use to try different things out in Go. 
// It is not part of any assignment or project.

package main


import "fmt"
import "time"

import "viewservice"

func main() {
	
	var start = time.Now()
	time.Sleep(5000 * time.Millisecond)
	var end = time.Now()

	var duration = end.Sub(start)

	fmt.Printf("Hi!\n")

	if duration > (viewservice.PingInterval * viewservice.DeadPings) {
		fmt.Printf("Late!\n")  
	}

	
	fmt.Printf("Hi!\n")
}