package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	HEADER_SIZE             = 9
	METADATA_SUBSECTION     = 0xFA
	DATABASE_SUBSECTION     = 0xFE
	HASH_TABLE_SIZE_SECTION = 0xFB
	DB_TYPE_STRING          = 0x0
	DB_KEY_EXP_MILLI        = 0xFC
	DB_KEY_EXP_SEC          = 0xFD
	EOF_SECTION             = 0xFF
)

func loadDataFromRDBFile() error {
	dbFile, err := os.Open(filepath.Join(cfg.dir, cfg.dbFileName))
	if err != nil {
		// File does not exist is not an error, DB starts out empty
		return nil
	}
	defer dbFile.Close()

	log.Printf(grey("=====Loading RDB file '%s'====="), filepath.Join(cfg.dir, cfg.dbFileName))
	fileBuf := bufio.NewReader(dbFile)

	// HEADER SECTION
	header := make([]byte, HEADER_SIZE)
	io.ReadFull(fileBuf, header)
	if !strings.HasPrefix(string(header), "REDIS") {
		return fmt.Errorf("unexpected RDB header '%s'", header)
	}
	log.Print(blue("HEADER:"))
	log.Printf(grey("  %s"), header)

	// METADATA SECTION (Denoted by 0xFA)
	log.Print(blue("METADATA:"))
	for {
		b, _ := fileBuf.ReadByte()
		if b == METADATA_SUBSECTION {
			name := parseStringEncodedValue(fileBuf)
			value := parseStringEncodedValue(fileBuf)
			log.Printf(grey("  %s='%s'"), name, value)
		} else {
			fileBuf.UnreadByte()
			break
		}
	}

	// DATABASE SUBSECTION (Denoted by 0xFE)
	exp := time.Time{}
	for {
		b, err := fileBuf.ReadByte()
		if err != nil {
			return err
		}

		switch b {
		case DATABASE_SUBSECTION:
			dbIndex := parseSizeEncodedValue(fileBuf)
			log.Printf(blue("DB INDEX %d:"), dbIndex)
		case HASH_TABLE_SIZE_SECTION:
			hashTableSize := parseSizeEncodedValue(fileBuf)
			hashTableSizeWithExpiry := parseSizeEncodedValue(fileBuf)
			log.Printf(grey("  TOTAL_SIZE=%d"), hashTableSize)
			log.Printf(grey("  EXPIRY_SIZE=%d"), hashTableSizeWithExpiry)
		case DB_TYPE_STRING:
			key := parseStringEncodedValue(fileBuf)
			value := Value{
				value: parseStringEncodedValue(fileBuf),
				exp:   exp,
			}
			cfg.db[key] = value
			log.Printf(grey("\t%s='%s', expires=%s"), key, value.value, value.exp.Format(time.RFC1123Z))
			exp = time.Time{} // Reset incase next key has no expiry
		case DB_KEY_EXP_MILLI:
			unixExpireTime := parseUnixTimeValue(fileBuf, "milli")
			exp = time.UnixMilli(int64(unixExpireTime))
		case DB_KEY_EXP_SEC:
			unixExpireTime := parseUnixTimeValue(fileBuf, "sec")
			exp = time.Unix(int64(unixExpireTime), 0)
		case EOF_SECTION:
			return nil
		}
	}
}

func parseSizeEncodedValue(buff *bufio.Reader) uint {
	b, _ := buff.ReadByte()

	size := uint(0)
	switch b & 0b1100_0000 {
	// If the first two bits are 0b00, the size is the remaining 6 bits of the byte.
	case 0:
		size = uint(b & 0b0011_1111)

	// If the first two bits are 0b01 the size is the next 14 bits
	case 0b0100_0000:
		bytes := []byte{b & 0b0011_1111}
		b, _ := buff.ReadByte()
		bytes = append(bytes, b)

		size = uint(binary.BigEndian.Uint16(bytes))

		// If the first two bits are 0b10, ignore the remaining 6 bits of the first byte.
		// The size is the next 4 bytes
	case 0b1000_0000:
		bytes := []byte{}
		for range 4 {
			b, _ := buff.ReadByte()
			bytes = append(bytes, b)
		}
		size = uint(binary.BigEndian.Uint32(bytes))
	}

	return size
}

func parseStringEncodedValue(buff *bufio.Reader) string {
	b, _ := buff.ReadByte()

	// Indicates that string is encoded as an integer
	if b&0b1100_0000 == 0b1100_0000 {
		switch b {
		// 8-bit integer
		case 0xC0:
			b, _ := buff.ReadByte()
			return strconv.Itoa(int(b))

		// 16-bit integer
		case 0xC1:
			bytes := []byte{}
			for range 2 {
				b, _ := buff.ReadByte()
				bytes = append(bytes, b)
			}
			return strconv.Itoa(int(binary.LittleEndian.Uint16(bytes)))

		// 32-bit integer
		case 0xC2:
			bytes := []byte{}
			for range 4 {
				b, _ := buff.ReadByte()
				bytes = append(bytes, b)
			}
			return strconv.Itoa(int(binary.LittleEndian.Uint32(bytes)))
		}
	}
	buff.UnreadByte()

	stringSize := parseSizeEncodedValue(buff)
	str := make([]byte, stringSize)
	io.ReadFull(buff, str)
	return string(str)
}

func parseUnixTimeValue(buff *bufio.Reader, units string) uint {
	bytes := []byte{}

	if units == "milli" {
		for range 8 {
			b, _ := buff.ReadByte()
			bytes = append(bytes, b)
		}
		return uint(binary.LittleEndian.Uint64(bytes))
	}

	if units == "sec" {
		for range 8 {
			b, _ := buff.ReadByte()
			bytes = append(bytes, b)
		}
		return uint(binary.LittleEndian.Uint32(bytes))
	}

	return 0
}
