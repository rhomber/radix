package bytesutil

import (
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteReader(t *testing.T) {
	type test struct {
		in           []byte
		l            int
		exp, expLeft []byte
		expErr       bool
	}

	tests := []test{
		{
			in: []byte{},
			l:  1, exp: []byte{}, expLeft: []byte{},
			expErr: true,
		},
		{
			in: []byte("foo"),
			l:  3, exp: []byte("foo"), expLeft: []byte{},
			expErr: true,
		},
		{
			in: []byte("foo"),
			l:  2, exp: []byte("fo"), expLeft: []byte("o"),
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			br := ByteReader(test.in)
			into := make([]byte, test.l)
			n, err := br.Read(into)
			assert.Equal(t, test.exp, into[:n])
			assert.Equal(t, test.expLeft, []byte(br))
			if test.expErr {
				assert.Equal(t, io.EOF, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
