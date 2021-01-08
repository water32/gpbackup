package integration

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queries LockTables", func() {
	Describe("BackupJobsLockTables", func() {
		var jobsConnectionPool *dbconn.DBConn
		BeforeEach(func() {
			gplog.SetVerbosity(gplog.LOGERROR) // turn off progress bar in the lock-table routine
			jobsConnectionPool = dbconn.NewDBConnFromEnvironment("testdb")
		})
		AfterEach(func() {
			if jobsConnectionPool != nil {
				jobsConnectionPool.Close()
			}
		})
		It("grab AccessShareLocks only on the main job worker thread when using unsafe worker parallelism", func() {
			_ = backupCmdFlags.Set(options.WORKER_PARALLELISM, "unsafe")
			jobsConnectionPool.MustConnect(3)
			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				jobsConnectionPool.MustBegin(connNum)
			}

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.backup_jobs_locktables(i int);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.backup_jobs_locktables;")

			// Need to defer commits AFTER deferring the DROP TABLE to prevent a hang on failure
			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				defer jobsConnectionPool.MustCommit(connNum)
			}

			tableRelations := []backup.Relation {backup.Relation{0, 0, "public", "backup_jobs_locktables"}}
			backup.LockTables(jobsConnectionPool, tableRelations)

			checkLockQuery := `SELECT count(*) FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname = 'backup_jobs_locktables' AND l.granted = 't' AND l.gp_segment_id = -1`
			var lockCount int
			_ = connectionPool.Get(&lockCount, checkLockQuery)
			Expect(lockCount).To(Equal(1))
		})
		It("grab unique AccessShareLocks for different job workers threads when using safe worker parallelism", func() {
			_ = backupCmdFlags.Set(options.WORKER_PARALLELISM, "safe")
			jobsConnectionPool.MustConnect(3)
			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				jobsConnectionPool.MustBegin(connNum)
			}

			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.backup_jobs_locktables1(i int);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.backup_jobs_locktables1;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.backup_jobs_locktables2(i int);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.backup_jobs_locktables2;")
			testhelper.AssertQueryRuns(connectionPool, "CREATE TABLE public.backup_jobs_locktables3(i int);")
			defer testhelper.AssertQueryRuns(connectionPool, "DROP TABLE public.backup_jobs_locktables3;")

			// Need to defer commits AFTER deferring the DROP TABLE to prevent a hang on failure
			for connNum := 0; connNum < jobsConnectionPool.NumConns; connNum++ {
				defer jobsConnectionPool.MustCommit(connNum)
			}

			tableRelations := []backup.Relation{backup.Relation{0, 0, "public", "backup_jobs_locktables1"},
				backup.Relation{0, 0, "public", "backup_jobs_locktables2"},
				backup.Relation{0, 0, "public", "backup_jobs_locktables3"}}
			backup.LockTables(jobsConnectionPool, tableRelations)

			checkLockQuery := `SELECT count(*) FROM (SELECT pid FROM pg_locks l, pg_class c, pg_namespace n WHERE l.relation = c.oid AND n.oid = c.relnamespace AND n.nspname = 'public' AND c.relname IN ('backup_jobs_locktables1', 'backup_jobs_locktables2', 'backup_jobs_locktables3') AND l.granted = 't' AND l.gp_segment_id = -1 GROUP BY pid) q`
			var lockCount int
			_ = connectionPool.Get(&lockCount, checkLockQuery)
			Expect(lockCount).To(Equal(3))
		})
	})
})
