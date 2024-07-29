package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"

	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
)

var (
	aesKey    = "********************************"
	key       = []byte(aesKey)
	plaintext = flag.String("plaintext", "", "insert password string")
	ctext     = flag.String("ciphertext", "", "insert ciphertext string")
)

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

// Aes Encrypt
func AesEncrypt(plaintext []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	plaintext = PKCS7Padding(plaintext, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	ciphertext := make([]byte, len(plaintext))
	blockMode.CryptBlocks(ciphertext, plaintext)
	return hex.EncodeToString(ciphertext), nil
}

// Aes Decryt
func AesDecrypt(ciphertext []byte) (string, error) {

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	plaintext := make([]byte, len(ciphertext))
	blockMode.CryptBlocks(plaintext, ciphertext)
	plaintext = PKCS7UnPadding(plaintext)
	return string(plaintext), nil
}

func main() {
	// parseing flag
	flag.Parse()

	if *plaintext == "" && *ctext == "" {
		log.Fatal("Nothing to do.")
	}

	if *plaintext != "" {
		//encrypt
		ciphertext, err := AesEncrypt([]byte(*plaintext))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(ciphertext)
	}

	if *ctext != "" {
		//decrypt
		plaintext, _ := hex.DecodeString(*ctext)
		pwd, err := AesDecrypt(plaintext)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(pwd)
	}

}
