package jupiter

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"time"
)

var src rand.Source

func init() {
	src = rand.NewSource(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randString(n int) string {
	b := make([]byte, n)
	l := len(letterBytes)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			if idx >= l {
				continue
			}
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func hashStrings(objects ...string) string {
	h := sha256.New()
	for _, o := range objects {
		io.WriteString(h, o)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func stringify(v interface{}, opts ...interface{}) string {
	if len(opts) > 0 {
		buf, err := json.MarshalIndent(v, "", "\t")
		if err != nil {
			return fmt.Sprintf("<error:%v>", err)
		}
		return string(buf)
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<error:%v>", err)
	}
	return string(buf)
}
