package pq

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestSimpleCopy(t *testing.T) {
	db, err := sql.Open("postgres", "user=test dbname=test password=test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS temp")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE temp (a int, b int)")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	cy := NewCopierFromTransaction(tx)
	err = cy.Start("COPY temp (a, b) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}

	data := fmt.Sprintf("12\t23\n24\t25\n")
	err = cy.Send([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	err = cy.Close()
	if err != nil {
		t.Fatal(err)
	}

	tx.Commit()

}

func TestErrCopy(t *testing.T) {
	db, err := sql.Open("postgres", "user=test dbname=test password=test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS temp")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE temp (a int, b int)")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	cy := NewCopierFromTransaction(tx)
	err = cy.Start("COPY temp (a, b) FROM STDIN")
	if err != nil {
		t.Fatal(err)
	}

	data := fmt.Sprintf("12\n24\t25\n")
	err = cy.Send([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	err = cy.Close()
	if err == nil {
		t.Fatal("Should Have Thrown An Error Due To Bad Input")
	}

	tx.Rollback()
}
