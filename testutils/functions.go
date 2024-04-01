package testutils

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/restore"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/sergi/go-diff/diffmatchpatch"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
)

/*
 * Functions for setting up the test environment and mocking out variables
 */

func SetupTestEnvironment() (*dbconn.DBConn, sqlmock.Sqlmock, *Buffer, *Buffer, *Buffer) {
	connectionPool, mock, testStdout, testStderr, testLogfile := testhelper.SetupTestEnvironment()

	// Default if not set is GPDB version `5.1.0`
	envTestGpdbVersion := os.Getenv("TEST_GPDB_VERSION")
	if envTestGpdbVersion != "" {
		testhelper.SetDBVersion(connectionPool, envTestGpdbVersion)
	}

	SetupTestCluster()
	backup.SetVersion("0.1.0")
	return connectionPool, mock, testStdout, testStderr, testLogfile
}

func SetupTestCluster() *cluster.Cluster {
	testCluster := SetDefaultSegmentConfiguration()
	backup.SetCluster(testCluster)
	restore.SetCluster(testCluster)
	testFPInfo := filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg", false)
	backup.SetFPInfo(testFPInfo)
	restore.SetFPInfo(testFPInfo)
	return testCluster
}

func SetupTestDbConn(dbname string) *dbconn.DBConn {
	conn := dbconn.NewDBConnFromEnvironment(dbname)
	conn.MustConnect(1)
	return conn
}

// Connects to specific segment in utility mode
func SetupTestDBConnSegment(dbname string, port int, host string, gpVersion dbconn.GPDBVersion) *dbconn.DBConn {

	if dbname == "" {
		gplog.Fatal(errors.New("No database provided"), "")
	}
	if port == 0 {
		gplog.Fatal(errors.New("No segment port provided"), "")
	}
	// Don't fail if no host is passed, as that implies connecting on the local host
	username := operating.System.Getenv("PGUSER")
	if username == "" {
		currentUser, _ := operating.System.CurrentUser()
		username = currentUser.Username
	}
	if host == "" {
		host := operating.System.Getenv("PGHOST")
		if host == "" {
			host, _ = operating.System.Hostname()
		}
	}

	conn := &dbconn.DBConn{
		ConnPool: nil,
		NumConns: 0,
		Driver:   &dbconn.GPDBDriver{},
		User:     username,
		DBName:   dbname,
		Host:     host,
		Port:     port,
		Tx:       nil,
		Version:  dbconn.GPDBVersion{},
	}

	var gpRoleGuc string
	if gpVersion.Before("7") {
		gpRoleGuc = "gp_session_role"
	} else {
		gpRoleGuc = "gp_role"
	}

	connStr := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable&statement_cache_capacity=0&%s=utility", conn.User, conn.Host, conn.Port, conn.DBName, gpRoleGuc)

	segConn, err := conn.Driver.Connect("pgx", connStr)
	if err != nil {
		gplog.FatalOnError(err)
	}
	conn.ConnPool = make([]*sqlx.DB, 1)
	conn.ConnPool[0] = segConn

	conn.Tx = make([]*sqlx.Tx, 1)
	conn.NumConns = 1
	version, err := dbconn.InitializeVersion(conn)
	if err != nil {
		gplog.FatalOnError(err)
	}
	conn.Version = version
	return conn
}

func SetDefaultSegmentConfiguration() *cluster.Cluster {
	configCoordinator := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "gpseg-1"}
	configSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "gpseg0"}
	configSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "gpseg1"}
	return cluster.NewCluster([]cluster.SegConfig{configCoordinator, configSegOne, configSegTwo})
}

func SetupTestFilespace(connectionPool *dbconn.DBConn, testCluster *cluster.Cluster) {
	remoteOutput := testCluster.GenerateAndExecuteCommand("Creating filespace test directory",
		cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return fmt.Sprintf("mkdir -p /tmp/test_dir")
		})
	if remoteOutput.NumErrors != 0 {
		Fail("Could not create filespace test directory on 1 or more hosts")
	}
	// Construct a filespace config like the one that gpfilespace generates
	filespaceConfigQuery := `COPY (SELECT hostname || ':' || dbid || ':/tmp/test_dir/' || preferred_role || content FROM gp_segment_configuration AS subselect) TO '/tmp/temp_filespace_config';`
	testhelper.AssertQueryRuns(connectionPool, filespaceConfigQuery)
	out, err := exec.Command("bash", "-c", "echo \"filespace:test_dir\" > /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace configuration: %s: %s", out, err.Error()))
	}
	out, err = exec.Command("bash", "-c", "cat /tmp/temp_filespace_config >> /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot finalize test filespace configuration: %s: %s", out, err.Error()))
	}
	// Create the filespace and verify it was created successfully
	out, err = exec.Command("bash", "-c", "gpfilespace --config /tmp/filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Cannot create test filespace: %s: %s", out, err.Error()))
	}
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		Fail("Filespace test_dir was not successfully created")
	}
}

func DestroyTestFilespace(connectionPool *dbconn.DBConn) {
	filespaceName := dbconn.MustSelectString(connectionPool, "SELECT fsname AS string FROM pg_filespace WHERE fsname = 'test_dir';")
	if filespaceName != "test_dir" {
		return
	}
	out, err := exec.Command("bash", "-c", "rm -rf /tmp/test_dir /tmp/filespace_config /tmp/temp_filespace_config").CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Could not remove test filespace directory and configuration files: %s: %s", out, err.Error()))
	} else {
		gplog.Info("Removed test filespace directory and configuration files: %s", out)
	}
	_, _ = connectionPool.Exec("DROP FILESPACE test_dir")
}

func DefaultMetadata(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) backup.ObjectMetadata {
	privileges := make([]backup.ACL, 0)
	if hasPrivileges {
		privileges = []backup.ACL{DefaultACLForType("testrole", objType)}
	}
	owner := ""
	if hasOwner {
		owner = "testrole"
	}
	comment := ""
	if hasComment {
		n := ""
		switch objType[0] {
		case 'A', 'E', 'I', 'O', 'U':
			n = "n"
		}
		comment = fmt.Sprintf("This is a%s %s comment.", n, strings.ToLower(objType))
	}
	securityLabelProvider := ""
	securityLabel := ""
	if hasSecurityLabel {
		securityLabelProvider = "dummy"
		securityLabel = "unclassified"
	}
	switch objType {
	case toc.OBJ_DOMAIN:
		objType = toc.OBJ_TYPE
	case toc.OBJ_FOREIGN_SERVER:
		objType = toc.OBJ_SERVER
	case toc.OBJ_MATERIALIZED_VIEW:
		objType = toc.OBJ_RELATION
	case toc.OBJ_SEQUENCE:
		objType = toc.OBJ_RELATION
	case toc.OBJ_TABLE:
		objType = toc.OBJ_RELATION
	case toc.OBJ_VIEW:
		objType = toc.OBJ_RELATION
	}
	return backup.ObjectMetadata{
		Privileges:            privileges,
		ObjectType:            objType,
		Owner:                 owner,
		Comment:               comment,
		SecurityLabelProvider: securityLabelProvider,
		SecurityLabel:         securityLabel,
	}

}

// objType should be an all-caps string like TABLE, INDEX, etc.
func DefaultMetadataMap(objType string, hasPrivileges bool, hasOwner bool, hasComment bool, hasSecurityLabel bool) backup.MetadataMap {
	return backup.MetadataMap{
		backup.UniqueID{ClassID: ClassIDFromObjectName(objType), Oid: 1}: DefaultMetadata(objType, hasPrivileges, hasOwner, hasComment, hasSecurityLabel),
	}
}

var objNameToClassID = map[string]uint32{
	toc.OBJ_AGGREGATE:                 1255,
	toc.OBJ_CAST:                      2605,
	toc.OBJ_COLLATION:                 3456,
	toc.OBJ_CONSTRAINT:                2606,
	toc.OBJ_CONVERSION:                2607,
	toc.OBJ_DATABASE:                  1262,
	toc.OBJ_DOMAIN:                    1247,
	toc.OBJ_EVENT_TRIGGER:             3466,
	toc.OBJ_EXTENSION:                 3079,
	toc.OBJ_FOREIGN_DATA_WRAPPER:      2328,
	toc.OBJ_FOREIGN_SERVER:            1417,
	toc.OBJ_FUNCTION:                  1255,
	toc.OBJ_INDEX:                     2610,
	toc.OBJ_LANGUAGE:                  2612,
	toc.OBJ_OPERATOR_CLASS:            2616,
	toc.OBJ_OPERATOR_FAMILY:           2753,
	toc.OBJ_OPERATOR:                  2617,
	toc.OBJ_PROTOCOL:                  7175,
	toc.OBJ_RESOURCE_GROUP:            6436,
	toc.OBJ_RESOURCE_QUEUE:            6026,
	toc.OBJ_ROLE:                      1260,
	toc.OBJ_RULE:                      2618,
	toc.OBJ_SCHEMA:                    2615,
	toc.OBJ_SEQUENCE:                  1259,
	toc.OBJ_TABLE:                     1259,
	toc.OBJ_TABLESPACE:                1213,
	toc.OBJ_TEXT_SEARCH_CONFIGURATION: 3602,
	toc.OBJ_TEXT_SEARCH_DICTIONARY:    3600,
	toc.OBJ_TEXT_SEARCH_PARSER:        3601,
	toc.OBJ_TEXT_SEARCH_TEMPLATE:      3764,
	toc.OBJ_TRIGGER:                   2620,
	toc.OBJ_TYPE:                      1247,
	toc.OBJ_USER_MAPPING:              1418,
	toc.OBJ_VIEW:                      1259,
	toc.OBJ_MATERIALIZED_VIEW:         1259,
}

func ClassIDFromObjectName(objName string) uint32 {
	return objNameToClassID[objName]

}
func DefaultACLForType(grantee string, objType string) backup.ACL {
	return backup.ACL{
		Grantee:    grantee,
		Select:     objType == toc.OBJ_PROTOCOL || objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Insert:     objType == toc.OBJ_PROTOCOL || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Update:     objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Delete:     objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Truncate:   objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		References: objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Trigger:    objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_FOREIGN_TABLE || objType == toc.OBJ_MATERIALIZED_VIEW,
		Usage:      objType == toc.OBJ_LANGUAGE || objType == toc.OBJ_SCHEMA || objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_FOREIGN_DATA_WRAPPER || objType == toc.OBJ_FOREIGN_SERVER,
		Execute:    objType == toc.OBJ_FUNCTION || objType == toc.OBJ_AGGREGATE,
		Create:     objType == toc.OBJ_DATABASE || objType == toc.OBJ_SCHEMA || objType == toc.OBJ_TABLESPACE,
		Temporary:  objType == toc.OBJ_DATABASE,
		Connect:    objType == toc.OBJ_DATABASE,
	}
}

func DefaultACLForTypeWithGrant(grantee string, objType string) backup.ACL {
	return backup.ACL{
		Grantee:             grantee,
		SelectWithGrant:     objType == toc.OBJ_PROTOCOL || objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		InsertWithGrant:     objType == toc.OBJ_PROTOCOL || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		UpdateWithGrant:     objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		DeleteWithGrant:     objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		TruncateWithGrant:   objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		ReferencesWithGrant: objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		TriggerWithGrant:    objType == toc.OBJ_TABLE || objType == toc.OBJ_VIEW || objType == toc.OBJ_MATERIALIZED_VIEW,
		UsageWithGrant:      objType == toc.OBJ_LANGUAGE || objType == toc.OBJ_SCHEMA || objType == toc.OBJ_SEQUENCE || objType == toc.OBJ_FOREIGN_DATA_WRAPPER || objType == toc.OBJ_FOREIGN_SERVER,
		ExecuteWithGrant:    objType == toc.OBJ_FUNCTION,
		CreateWithGrant:     objType == toc.OBJ_DATABASE || objType == toc.OBJ_SCHEMA || objType == toc.OBJ_TABLESPACE,
		TemporaryWithGrant:  objType == toc.OBJ_DATABASE,
		ConnectWithGrant:    objType == toc.OBJ_DATABASE,
	}
}

func DefaultACLWithout(grantee string, objType string, revoke ...string) backup.ACL {
	defaultACL := DefaultACLForType(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.Select = false
		case "INSERT":
			defaultACL.Insert = false
		case "UPDATE":
			defaultACL.Update = false
		case "DELETE":
			defaultACL.Delete = false
		case "TRUNCATE":
			defaultACL.Truncate = false
		case "REFERENCES":
			defaultACL.References = false
		case "TRIGGER":
			defaultACL.Trigger = false
		case "EXECUTE":
			defaultACL.Execute = false
		case "USAGE":
			defaultACL.Usage = false
		case "CREATE":
			defaultACL.Create = false
		case "TEMPORARY":
			defaultACL.Temporary = false
		case "CONNECT":
			defaultACL.Connect = false
		}
	}
	return defaultACL
}

func DefaultACLWithGrantWithout(grantee string, objType string, revoke ...string) backup.ACL {
	defaultACL := DefaultACLForTypeWithGrant(grantee, objType)
	for _, priv := range revoke {
		switch priv {
		case "SELECT":
			defaultACL.SelectWithGrant = false
		case "INSERT":
			defaultACL.InsertWithGrant = false
		case "UPDATE":
			defaultACL.UpdateWithGrant = false
		case "DELETE":
			defaultACL.DeleteWithGrant = false
		case "TRUNCATE":
			defaultACL.TruncateWithGrant = false
		case "REFERENCES":
			defaultACL.ReferencesWithGrant = false
		case "TRIGGER":
			defaultACL.TriggerWithGrant = false
		case "EXECUTE":
			defaultACL.ExecuteWithGrant = false
		case "USAGE":
			defaultACL.UsageWithGrant = false
		case "CREATE":
			defaultACL.CreateWithGrant = false
		case "TEMPORARY":
			defaultACL.TemporaryWithGrant = false
		case "CONNECT":
			defaultACL.ConnectWithGrant = false
		}
	}
	return defaultACL
}

/*
 * Wrapper functions around gomega operators for ease of use in tests
 */

func SliceBufferByEntries(entries []toc.MetadataEntry, buffer *Buffer) ([]string, string) {
	contents := buffer.Contents()
	hunks := make([]string, 0)
	length := uint64(len(contents))
	var end uint64
	for _, entry := range entries {
		start := entry.StartByte
		end = entry.EndByte
		if start > length {
			start = length
		}
		if end > length {
			end = length
		}
		hunks = append(hunks, string(contents[start:end]))
	}
	return hunks, string(contents[end:])
}

func CompareSlicesIgnoringWhitespace(actual []string, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if strings.TrimSpace(actual[i]) != expected[i] {
			return false
		}
	}
	return true
}

func formatEntries(entries []toc.MetadataEntry, slice []string) string {
	formatted := ""
	for i, item := range slice {
		formatted += fmt.Sprintf("%v -> %q\n", entries[i], item)
	}
	return formatted
}

func formatContents(slice []string) string {
	formatted := ""
	for _, item := range slice {
		formatted += fmt.Sprintf("%q\n", item)
	}
	return formatted
}

func formatDiffs(actual []string, expected []string) string {
	dmp := diffmatchpatch.New()
	diffs := ""
	for idx := range actual {
		diffs += dmp.DiffPrettyText(dmp.DiffMain(expected[idx], actual[idx], false))
	}
	return diffs
}

func AssertBufferContents(entries []toc.MetadataEntry, buffer *Buffer, expected ...string) {
	if len(entries) == 0 {
		Fail("TOC is empty")
	}
	hunks, remaining := SliceBufferByEntries(entries, buffer)
	if remaining != "" {
		Fail(fmt.Sprintf("Buffer contains extra contents that are not being counted by TOC:\n%s\n\nActual TOC entries were:\n\n%s", remaining, formatEntries(entries, hunks)))
	}
	ok := CompareSlicesIgnoringWhitespace(hunks, expected)
	if !ok {
		Fail(fmt.Sprintf("Actual TOC entries:\n\n%s\n\ndid not match expected contents (ignoring whitespace):\n\n%s \n\nDiff:\n>>%s\x1b[31m<<", formatEntries(entries, hunks), formatContents(expected), formatDiffs(hunks, expected)))
	}
}

func ExpectEntry(entries []toc.MetadataEntry, index int, schema, referenceObject, name, objectType string) {
	Expect(len(entries)).To(BeNumerically(">", index))
	structmatcher.ExpectStructsToMatchExcluding(entries[index], toc.MetadataEntry{Schema: schema, Name: name, ObjectType: objectType, ReferenceObject: referenceObject, StartByte: 0, EndByte: 0}, "StartByte", "EndByte", "Tier")
}

func ExpectEntryCount(entries []toc.MetadataEntry, index int) {
	Expect(len(entries)).To(BeNumerically("==", index))
}

func ExecuteSQLFile(connectionPool *dbconn.DBConn, filename string) {
	connStr := []string{
		"-U", connectionPool.User,
		"-d", connectionPool.DBName,
		"-h", connectionPool.Host,
		"-p", fmt.Sprintf("%d", connectionPool.Port),
		"-f", filename,
		"-v", "ON_ERROR_STOP=1",
		"-q",
	}
	out, err := exec.Command("psql", connStr...).CombinedOutput()
	if err != nil {
		Fail(fmt.Sprintf("Execution of SQL file encountered an error: %s", out))
	}
}

func BufferLength(buffer *Buffer) uint64 {
	return uint64(len(buffer.Contents()))
}

func OidFromCast(connectionPool *dbconn.DBConn, castSource uint32, castTarget uint32) uint32 {
	query := fmt.Sprintf("SELECT c.oid FROM pg_cast c WHERE castsource = '%d' AND casttarget = '%d'", castSource, castTarget)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func OidFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) uint32 {
	catalogTable := params.CatalogTable
	if params.OidTable != "" {
		catalogTable = params.OidTable
	}
	schemaStr := ""
	if schemaName != "" {
		schemaStr = fmt.Sprintf(" AND %s = (SELECT oid FROM pg_namespace WHERE nspname = '%s')", params.SchemaField, schemaName)
	}
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'%s", catalogTable, params.NameField, objectName, schemaStr)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}
	return result.Oid
}

func UniqueIDFromObjectName(connectionPool *dbconn.DBConn, schemaName string, objectName string, params backup.MetadataQueryParams) backup.UniqueID {
	query := fmt.Sprintf("SELECT '%s'::regclass::oid", params.CatalogTable)
	result := struct {
		Oid uint32
	}{}
	err := connectionPool.Get(&result, query)
	if err != nil {
		Fail(fmt.Sprintf("Execution of query failed: %v", err))
	}

	return backup.UniqueID{ClassID: result.Oid, Oid: OidFromObjectName(connectionPool, schemaName, objectName, params)}
}

func GetUserByID(connectionPool *dbconn.DBConn, oid uint32) string {
	return dbconn.MustSelectString(connectionPool, fmt.Sprintf("SELECT rolname AS string FROM pg_roles WHERE oid = %d", oid))
}

func CreateSecurityLabelIfGPDB6(connectionPool *dbconn.DBConn, objectType string, objectName string) {
	if connectionPool.Version.AtLeast("6") {
		testhelper.AssertQueryRuns(connectionPool, fmt.Sprintf("SECURITY LABEL FOR dummy ON %s %s IS 'unclassified';", objectType, objectName))
	}
}

func SkipIfBefore6(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before("6") {
		Skip("Test only applicable to GPDB6 and above")
	}
}

func SkipIfBefore7(connectionPool *dbconn.DBConn) {
	if connectionPool.Version.Before("7") {
		Skip("Test only applicable to GPDB7 and above")
	}
}

func InitializeTestTOC(buffer io.Writer, which string) (*toc.TOC, *utils.FileWithByteCount) {
	tocfile := &toc.TOC{}
	tocfile.InitializeMetadataEntryMap()
	backupfile := utils.NewFileWithByteCount(buffer)
	backupfile.Filename = which
	return tocfile, backupfile
}
