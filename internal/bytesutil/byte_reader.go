package bytesutil

import "io"

type ByteReader []byte

func (r *ByteReader) Read(b []byte) (int, error) {
	n := copy(b, *r)
	var err error
	if n == len(*r) {
		err = io.EOF
	}
	*r = (*r)[n:]
	return n, err
}
