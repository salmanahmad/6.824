
// This is just a simple test program that 
// I use to try different things out in Go. 
// It is not part of any assignment or project.

package main


import "fmt"
import "time"
import "viewservice"

func main() {
	
  view1 := viewservice.View {1, "Primary", "Backup"}
  view2 := viewservice.View {0, "Primary", "Backup"}
  
  if(view1 == view2) {
    fmt.Printf("They are the same\n");
  } else {
    fmt.Printf("They are NOT the same\n");
  }
  
  return 
  
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