package adapter

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
)

var aesKey = "********************************"
var key = []byte(aesKey)

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) ([]byte, error) {
	length := len(origData)
	unpadding := int(origData[length-1])
	if unpadding > length || unpadding <= 0 {
		return nil, errors.New("Invalid padding size.")
	}

	return origData[:(length - unpadding)], nil
}

// Aes Encrypt
func AesEncrypt(pwd string) (string, error) {

	plaintext := []byte(pwd)

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
func AesDecrypt(pwd string) (string, error) {

	ciphertext, err := hex.DecodeString(pwd)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	if len(ciphertext) < blockSize {
		return "", errors.New("Ciphertext length is less than the AES block size.")
	}

	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	plaintext := make([]byte, len(ciphertext))
	blockMode.CryptBlocks(plaintext, ciphertext)
	plaintext, err = PKCS7UnPadding(plaintext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}
