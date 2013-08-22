package pq

import "database/sql/driver"

type Copier struct {
	c       *conn
	sending bool
}

/*
//some statemnt compat functions
func (cy *Copier) Exec(args []driver.Value) (driver.Result, error) {
	bytes, ok := args[0].([]byte)
	if !ok {
		errorf("Copy data statements require byte parameters")
	}
	return nil, cy.Send(bytes)
}

func (cy *Copier) NumInput() int {
	return 1
}

func (cy *Copier) Query(args []driver.Value) (driver.Result, error) {
	errorf("Copy data statements never accept queries")
	return nil, nil
}
*/

func NewCopier(name string) *Copier {
	cy := Copier{}
	err := cy.Open(name)
	if err != nil {
		panic(err)
	}
	return &cy
}

func NewCopierFromConn(con driver.Conn) *Copier {
	conn := con.(*conn)
	cy := Copier{c: conn}
	return &cy
}

func NewCopierFromTransaction(t driver.Tx) *Copier {
	conn := t.(*conn)
	cy := Copier{c: conn}
	return &cy
}

func (cop *Copier) Open(name string) error {
	d := drv{}
	con, err := d.Open(name)
	if err == nil {
		cop.c = con.(*conn)
	}
	return err
}

func (cy *Copier) Start(q string) (err error) {
	defer errRecover(&err)

	b := newWriteBuf('Q')
	b.string(q)
	cy.c.send(b)
	for {
		t, r := cy.c.recv1()
		switch t {
		case 'N':
			//ignore
		case 'E':
			err = parseError(r)
			return err
		case 'G':
			cy.sending = true
			return nil
		default:
			errorf("unknown response for start copy: %q %v", t, r.string())
		}
	}
}

func (cy *Copier) Send(buf []byte) (err error) {
	//println("Sending %v", string(buf))
	if !cy.sending {
		errorf("Trying to send copy data when not in send mode")
	}

	b := newWriteBuf('d')
	b.bytes(buf)
	cy.c.send(b)
	return nil
}

func (cy *Copier) Close() (err error) {
	if !cy.sending {
		errorf("Trying to send copy data when not in send mode")
	}

	b := newWriteBuf('c')
	cy.c.send(b)
	for {
		t, r := cy.c.recv1()
		switch t {
		case 'C':
		case 'E':
			err = parseError(r)
			return err
		case 'Z':
			// done
			return
		default:
			errorf("Unknown response for end copy data: %q", t)
		}
	}
}
