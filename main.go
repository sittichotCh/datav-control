package dbcontrol

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"strings"
	"time"
)

func NewMigrateDatabaseConfig() *migrateDatabaseConfig {
	return &migrateDatabaseConfig{}
}

func MigrateDatabase(config *migrateDatabaseConfig) error {
	conn, closeFn, err := connectDB(config)
	if err != nil {
		return err
	}
	defer closeFn()

	if err := initTableChangelog(conn, config); err != nil {
		return err
	}

	changelogs, err := loadAllChangelog(config.Path)
	if err != nil {
		return err
	}

	for _, changelog := range changelogs {
		query, err := generateQuery(changelog)
		if err != nil {
			return err
		}

		verified, err := verifyChangelog(conn, changelog.Id, query)
		if err != nil {
			return err
		}
		if verified {
			continue
		}

		if _, err := conn.Query(query); err != nil {
			return err
		}

		if err := migrateChangelogSuccess(conn, query, changelog); err != nil {
			return err
		}
	}
	return nil
}

func verifyChangelog(conn *sql.DB, idChangelog string, query string) (bool, error) {
	md5Changelog := toMd5(query)
	q := findChangelogById()

	id := ""
	md5Sum := ""
	stat, err := conn.Prepare(q)
	if err != nil {
		return false, err
	}
	rows, err := stat.Query(idChangelog)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		if err := rows.Scan(&id, &md5Sum); err != nil {
			return false, err
		}

		if md5Sum != md5Changelog {
			message := fmt.Sprintf("chanaglog id: %s md5 sum is %s but %s", idChangelog, md5Sum, md5Changelog)
			return false, errors.New(message)
		}

		return true, nil
	}

	return false, nil
}

func toMd5(query string) string {
	bytes := []byte(query)
	md5Sum := md5.Sum(bytes)
	return hex.EncodeToString(md5Sum[:])
}

func migrateChangelogSuccess(conn *sql.DB, query string, changelog *changelog) error {
	insertQuery := insertChangelogQuery(&changelogEntity{
		Id:            &changelog.Id,
		Description:   &changelog.Description,
		Md5Sum:        ToPtrString(toMd5(query)),
		QueryLog:      ToPtrString(query),
		ExecutionTime: ToPtrTime(time.Now()),
	})

	_, err := conn.Exec(insertQuery)
	if err != nil {
		return err
	}

	return nil
}

func insertChangelogQuery(entity *changelogEntity) string {
	columns := make([]string, 0)
	values := make([]string, 0)
	if entity.Id != nil {
		columns = append(columns, "id")
		values = append(values, "\""+ToString(entity.Id)+"\"")
	}

	if entity.Description != nil {
		columns = append(columns, "description")
		values = append(values, "\""+ToString(entity.Description)+"\"")
	}

	if entity.QueryLog != nil {
		columns = append(columns, "query_log")
		values = append(values, "\""+ToString(entity.QueryLog)+"\"")
	}

	if entity.Md5Sum != nil {
		columns = append(columns, "md5_sum")
		values = append(values, "\""+ToString(entity.Md5Sum)+"\"")
	}

	if entity.ExecutionTime != nil {
		columns = append(columns, "execution_time")
		dateTime := entity.ExecutionTime.Format("2006-01-02T15:04:05")
		values = append(values, "\""+dateTime+"\"")
	}

	columnsStr := strings.Join(columns, ",")
	valuesStr := strings.Join(values, ",")
	return fmt.Sprintf("INSERT INTO database_changelog (%s) VALUES (%s)", columnsStr, valuesStr)
}

func findChangelogById() string {
	return "select id, md5_sum from database_changelog where id = ?"
}

func generateQuery(changelog *changelog) (string, error) {
	if changelog.CreateTable != nil {
		return generateCreateTableQuery(changelog.CreateTable)
	}

	return "", nil
}

func generateCreateTableQuery(command *createTable) (string, error) {
	if command == nil {
		return "", errors.New("create table command cannot be null")
	}

	columnsStr := make([]string, len(command.Columns))
	for i, column := range command.Columns {
		nonNull := ""
		if column.NonNullable {
			nonNull = "NOT NULL"
		}

		defaultValue := ""
		if v := column.DefaultValue; v != "" {
			defaultValue = fmt.Sprintf("DEFAULT '%s'", v)
		}
		columnsStr[i] = fmt.Sprintf("%s %s %s %s", column.ColumnName, column.ColumnType, nonNull, defaultValue)
	}
	columnQuery := strings.Join(columnsStr, ",")
	pkStr := strings.Join(command.PrimaryKeys, ",")
	pkQuery := fmt.Sprintf("PRIMARY KEY (%s)", pkStr)

	return fmt.Sprintf("create table %s (%s, %s)", command.TableName, columnQuery, pkQuery), nil
}

func generateAlterTableQuery(command *alterTable) string {

	return ""
}

func initTableChangelog(conn *sql.DB, config *migrateDatabaseConfig) error {
	if err := createDatabaseChangelog(conn); err != nil {
		return err
	}

	return nil
}

func connectDB(config *migrateDatabaseConfig) (*sql.DB, func() error, error) {
	var dbPW string
	{
		if config.DatabasePassword == "" {
			dbPW = ""
		} else {
			dbPW = fmt.Sprintf(":%s", config.DatabasePassword)
		}
	}
	dsn := fmt.Sprintf("%s%s@%s(%s)/%s", config.DatabaseId, dbPW, config.DatabaseProtocol, config.DatabaseUrl, config.DatabaseSchema)
	db, err := sql.Open("mysql", dsn)

	return db, func() error {
		err := db.Close()
		if err != nil {
			return err
		}

		return nil
	}, err
}

func createDatabaseChangelog(db *sql.DB) error {
	_, err := db.Exec("create table if not exists database_changelog" +
		"(" +
		"id varchar(255) primary key, " +
		"description text, " +
		"execution_time datetime(3) NOT NULL," +
		"md5_sum varchar(32) NOT NULL," +
		"query_log text NOT NULL" +
		")")
	if err != nil {
		return err
	}

	return nil
}

func loadAllChangelog(path string) ([]*changelog, error) {
	pathChangelogs := make([]string, 0)
	if err := loadJsonFile(path, &pathChangelogs); err != nil {
		return nil, err
	}

	result := make([]*changelog, 0)
	for _, pathChangelog := range pathChangelogs {
		changelogs := make([]*changelog, 0)
		if err := loadJsonFile(pathChangelog, &changelogs); err != nil {
			return nil, err
		}

		result = append(result, changelogs...)
	}

	return result, nil
}

func loadJsonFile(path string, v any) error {
	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	if err := json.Unmarshal(byteValue, v); err != nil {
		panic(err)
	}

	return nil
}
