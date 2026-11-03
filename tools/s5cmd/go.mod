// Pins the s5cmd release (github.com/peak/s5cmd/v2 below) and every module it is built with; go.sum holds
// their checksums. To update, merge the Dependabot PR or run `go get github.com/peak/s5cmd/v2@<version>` here.
module github.com/cometadata/comet-data-infrastructure/tools/s5cmd

go 1.27.3

tool github.com/peak/s5cmd/v2

require (
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aws/aws-sdk-go v1.44.298 // indirect
	github.com/cheggaaa/pb/v3 v3.1.4 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/karrick/godirwalk v1.15.3 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/lanrat/extsort v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/peak/s5cmd/v2 v2.3.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/termie/go-shutil v0.0.0-20140729215957-bcacb06fecae // indirect
	github.com/urfave/cli/v2 v2.11.2 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.uber.org/mock v0.4.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
)
