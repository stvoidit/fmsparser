package main

import (
	"compress/bzip2"
	"fmsparser"
	"fmt"
	"os"
)

func main() {
	// exemple1()
	// exemple2()
	// exemple3()
}

// применяем чтение к уже существующему архиву
func exemple1() {
	db, err := fmsparser.NewStore("localhost", "5432", "postgres", "postgres", "fms")
	if err != nil {
		panic(err)
	}
	var p = fmsparser.NewParser(2, 100000, db)
	rc, _ := os.Open(`exemple/list_of_expired_passports.csv_0.bz2`)
	defer rc.Close()
	r := bzip2.NewReader(rc)
	fmt.Println("read file complite")
	if err := p.InsertData(r); err != nil {
		panic(err)
	}
}

// отдельно качаем, передаем ридер на чтение и инсерт
func exemple2() {
	db, err := fmsparser.NewStore("localhost", "5432", "postgres", "postgres", "fms")
	if err != nil {
		panic(err)
	}
	var p = fmsparser.NewParser(2, 100000, db)
	r, err := p.FetchArchive()
	if err != nil {
		panic(err)
	}
	fmt.Println("fetch complite")
	if err = p.InsertData(r); err != nil {
		panic(err)
	}
}

// сразу качаем, читаем, инсертим
func exemple3() {
	db, err := fmsparser.NewStore("localhost", "5432", "postgres", "postgres", "fms")
	if err != nil {
		panic(err)
	}
	var p = fmsparser.NewParser(2, 100000, db)
	if err := p.FetchAndInsert(); err != nil {
		panic(err)
	}
}
