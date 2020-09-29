package helpers

import (
	"math"
	"math/rand"
	"time"
)

func randomString(length int, charset string) string {
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func RandString(length int) string {
	return randomString(length, "abcdefghijklmnopqrstuvwxyz")
}

func RandNumericString(length int) string {
	return randomString(length, "01234567890")
}
func RandInt(minimum int, maximum int) int {
	rand.Seed(time.Now().UnixNano())

	return rand.Intn(maximum - minimum + 1) + minimum
}

func RandIntExcluding(minimum int, maximum int, excluding int) int {
	rand.Seed(time.Now().UnixNano())

	n := RandInt(minimum, maximum-1)
	if n >= excluding {
		n += 1
	}

	return n
}

//Returns a random float between [minimum;maximum] and rounds to precision precision.
func RandFloat(minimum float64, maximum float64, precision int) float64 {
	p:= math.Pow(10, float64(precision))
	return math.Round((minimum + rand.Float64()*(maximum-minimum))*p)/p
}

//Puts string tmp1 at a random position of tmp string
func RandOriginal(tmp string, tmp1 string) string {
	position := rand.Intn( len(tmp)-len(tmp1) )
	return tmp[:position] + tmp1 + tmp[position+len(tmp1):]
}

func SelectUniqueIds(numUnique int, minimum int, maximum int) []int {
	var res []int
	var add_ int
	for i:=0; i<numUnique; i++ {
		rand_ := RandInt(minimum, maximum)

		add_ = 1
		for _, item := range res {
			if item == rand_ {
				add_=0
				break
			}
		}

		if len(res) == 0 || add_ == 1 {
			res = append(res, rand_)
		} else {
			i--
		}

	}

	return res
}

