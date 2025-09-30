package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var outputPath string

	var rootCmd = &cobra.Command{
		Use:   "rbd-diff-apply",
		Short: "A tool to apply Ceph diffs to a block device",
		Run: func(cmd *cobra.Command, args []string) {
			if outputPath == "" {
				fmt.Println("Output block device path is required")
				os.Exit(1)
			}
			ParseStdin(outputPath)
		},
	}

	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Path to the output block device")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)

		os.Exit(1)
	}
}
