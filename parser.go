package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
)

var (
	// Record tags as bytes
	FromSnap    byte = 0x66 // f
	ToSnap      byte = 0x74 // t
	Size        byte = 0x73 // s
	UpdatedData byte = 0x77 // w
	ZeroData    byte = 0x7A // z
	End         byte = 0x65 // e
	Header           = []byte("rbd diff v1\n")
)

type Metadata struct {
	fromSnapNameLength int
	fromSnapName       string
	toSnapNameLength   int
	toSnapName         string
	size               int
}

func readBytes(reader *bufio.Reader, n int) ([]byte, int) {
	bytes := make([]byte, n)
	br, err := io.ReadFull(reader, bytes)
	if err != nil {
		return nil, 0
	}
	if br != n {
		log.Fatal("Could not read expected number of bytes")
		return nil, n
	}

	return bytes, br
}

func writeBytes(f *os.File, offset int, dataBytes []byte) {
	length := len(dataBytes)

	// Seek to the correct offset
	_, err := f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		log.Fatalf("Failed to seek to offset %d: %v", offset, err)
	}

	// Write the data
	written, err := f.Write(dataBytes)
	if err != nil || written != length {
		log.Fatalf("Failed to write data to block device: %v", err)
	}
	log.Printf("  Successfully wrote %d bytes to device at offset %d", written, offset)
}

func ParseStdin(outputPath string) {
	//var headerSeen bool = false
	//var metadataSeen bool = false
	//var dataSeen bool = false

	m := Metadata{}
	byteTotal := 0

	// Open the block device for writing

	f, err := os.OpenFile(outputPath, os.O_WRONLY, 0)
	if err != nil {
		log.Fatalf("Failed to open block device: %v", err)
	}
	defer f.Close()

	reader := bufio.NewReader(os.Stdin)
	// Find header.
	for {
		headerBytes := make([]byte, len(Header))
		n, err := reader.Read(headerBytes)
		if err != nil || n != len(Header) || !bytes.Equal(headerBytes, Header) {
			// handle invalid or missing header
			return
		}
		byteTotal += n
		log.Println("Header found in input.")
		log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
		break // exit header parsing loop
	}
	// Parse metadata records.
	var metadataDone bool = false
	for {
		if metadataDone {
			break
		}
		log.Printf("Metadata: fromSnapNameLength=%d, fromSnapName=%q, toSnapNameLength=%d, toSnapName=%q, size=%d",
			m.fromSnapNameLength, m.fromSnapName, m.toSnapNameLength, m.toSnapName, m.size)

		b, br := readBytes(reader, 1)
		byteTotal += br
		switch b[0] {
		case FromSnap:
			// The next 4 bytes are snap name length little endian int32
			fromSnapNameLengthBytes, br := readBytes(reader, 4)
			byteTotal += br

			m.fromSnapNameLength = int(fromSnapNameLengthBytes[0]) | int(fromSnapNameLengthBytes[1])<<8 | int(fromSnapNameLengthBytes[2])<<16 | int(fromSnapNameLengthBytes[3])<<24
			// The next N bytes are the snap name
			fromSnapNameBytes, br := readBytes(reader, m.fromSnapNameLength)
			byteTotal += br

			m.fromSnapName = string(fromSnapNameBytes)
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
		case ToSnap:
			// The next 4 bytes are snap name length little endian int32
			toSnapNameLengthBytes, br := readBytes(reader, 4)
			byteTotal += br

			m.toSnapNameLength = int(toSnapNameLengthBytes[0]) | int(toSnapNameLengthBytes[1])<<8 | int(toSnapNameLengthBytes[2])<<16 | int(toSnapNameLengthBytes[3])<<24
			// The next N bytes are the snap name
			toSnapNameBytes, br := readBytes(reader, m.toSnapNameLength)
			byteTotal += br

			m.toSnapName = string(toSnapNameBytes)
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)

		case Size:
			sizeBytes, br := readBytes(reader, 8)
			byteTotal += br

			m.size = int(sizeBytes[0]) | int(sizeBytes[1])<<8 | int(sizeBytes[2])<<16 | int(sizeBytes[3])<<24 |
				int(sizeBytes[4])<<32 | int(sizeBytes[5])<<40 | int(sizeBytes[6])<<48 | int(sizeBytes[7])<<56
			log.Printf("Metadata: fromSnapNameLength=%d, fromSnapName=%q, toSnapNameLength=%d, toSnapName=%q, size=%d",
				m.fromSnapNameLength, m.fromSnapName, m.toSnapNameLength, m.toSnapName, m.size)
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
			metadataDone = true

		default:
			// handle unknown tag
		}
	}

	var dataDone bool = false
	// Parse data records.
	for {
		if dataDone {
			break
		}
		b, err := reader.ReadByte()
		if err != nil {
			break
		}
		byteTotal += 1
		switch b {
		case UpdatedData:
			log.Println("UpdatedData record found.")
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
			// 64 bit offset
			offsetBytes, br := readBytes(reader, 8)
			byteTotal += br
			offset := int(offsetBytes[0]) | int(offsetBytes[1])<<8 | int(offsetBytes[2])<<16 | int(offsetBytes[3])<<24 |
				int(offsetBytes[4])<<32 | int(offsetBytes[5])<<40 | int(offsetBytes[6])<<48 | int(offsetBytes[7])<<56
			log.Printf("  Offset: %d | %x", offset, offset)
			// 64 bit length
			lengthBytes, br := readBytes(reader, 8)
			byteTotal += br
			length := int(lengthBytes[0]) | int(lengthBytes[1])<<8 | int(lengthBytes[2])<<16 | int(lengthBytes[3])<<24 |
				int(lengthBytes[4])<<32 | int(lengthBytes[5])<<40 | int(lengthBytes[6])<<48 | int(lengthBytes[7])<<56
			log.Printf("  Length: %d | %x", length, length)
			// N bytes of data
			dataBytes, br := readBytes(reader, length)
			byteTotal += br
			log.Printf("  Data (truncated): %x", dataBytes[:16])

			writeBytes(f, offset, dataBytes)

		case ZeroData:
			log.Println("ZeroData record found.")
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
			// 64 bit offset
			offsetBytes, br := readBytes(reader, 8)
			byteTotal += br
			offset := int(offsetBytes[0]) | int(offsetBytes[1])<<8 | int(offsetBytes[2])<<16 | int(offsetBytes[3])<<24 |
				int(offsetBytes[4])<<32 | int(offsetBytes[5])<<40 | int(offsetBytes[6])<<48 | int(offsetBytes[7])<<56
			log.Printf("  Offset: %d | %x", offset, offset)
			// 64 bit length
			lengthBytes, br := readBytes(reader, 8)
			byteTotal += br
			length := int(lengthBytes[0]) | int(lengthBytes[1])<<8 | int(lengthBytes[2])<<16 | int(lengthBytes[3])<<24 |
				int(lengthBytes[4])<<32 | int(lengthBytes[5])<<40 | int(lengthBytes[6])<<48 | int(lengthBytes[7])<<56
			log.Printf("  Length: %d | %x", length, length)

			zeroBytes := make([]byte, length)
			writeBytes(f, offset, zeroBytes)

		case End:
			log.Println("End record found, finishing.")
			log.Printf("Current byteTotal: %d | %x", byteTotal, byteTotal)
			dataDone = true

		default:
			// handle unknown tag
		}
	}
}
