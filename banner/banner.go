package banner

import (
	"fmt"
)

// prints the version message
const version = "v0.0.2"

func PrintVersion() {
	fmt.Printf("Current commoncrawlscans version %s\n", version)
}

// Prints the Colorful banner
func PrintBanner() {
	banner := `
                                                                                 __                               
  _____ ____   ____ ___   ____ ___   ____   ____   _____ _____ ____ _ _      __ / /_____ _____ ____ _ ____   _____
 / ___// __ \ / __  __ \ / __  __ \ / __ \ / __ \ / ___// ___// __  /| | /| / // // ___// ___// __  // __ \ / ___/
/ /__ / /_/ // / / / / // / / / / // /_/ // / / // /__ / /   / /_/ / | |/ |/ // /(__  )/ /__ / /_/ // / / /(__  ) 
\___/ \____//_/ /_/ /_//_/ /_/ /_/ \____//_/ /_/ \___//_/    \__,_/  |__/|__//_//____/ \___/ \__,_//_/ /_//____/
`
	fmt.Printf("%s\n%70s\n\n", banner, "Current commoncrawlscans version "+version)
}
