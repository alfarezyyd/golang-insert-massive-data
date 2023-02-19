package app

import (
	"database/sql"
	"golang-insert-massive-data/helper"
	"time"
)

func SetupNewDBConn() *sql.DB {
	db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/golang_insert_massive_data")
	helper.PanicIfError(err)

	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(10 * time.Minute)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(100)
	return db
}
