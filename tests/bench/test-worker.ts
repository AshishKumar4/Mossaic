import type { EnvCore } from "@shared/types";
import { ShardDO } from "@core/objects/shard/shard-do";
import { UserDOCore } from "@core/objects/user/user-do-core";
import { CountingSqlStorage, type SqlMetrics } from "./counting-sql-storage";

export class BenchmarkUserDO extends UserDOCore {
  private sqlCounter: CountingSqlStorage | undefined;

  async benchmarkResetSqlMetrics(): Promise<void> {
    if (this.sqlCounter === undefined) {
      this.sqlCounter = new CountingSqlStorage();
      this.sql = this.sqlCounter.wrap(this.sql);
    }
    this.sqlCounter.reset();
  }

  async benchmarkSqlMetrics(): Promise<SqlMetrics> {
    if (this.sqlCounter === undefined) {
      throw new Error("SQL metrics were not started");
    }
    return this.sqlCounter.snapshot();
  }
}

export class BenchmarkShardDO extends ShardDO {
  private sqlCounter: CountingSqlStorage | undefined;

  async benchmarkResetSqlMetrics(): Promise<void> {
    if (this.sqlCounter === undefined) {
      this.sqlCounter = new CountingSqlStorage();
      this.sql = this.sqlCounter.wrap(this.sql);
    }
    this.sqlCounter.reset();
  }

  async benchmarkSqlMetrics(): Promise<SqlMetrics> {
    if (this.sqlCounter === undefined) {
      throw new Error("SQL metrics were not started");
    }
    return this.sqlCounter.snapshot();
  }
}

export default {
  fetch(): Response {
    return new Response("Mossaic storage benchmark");
  },
} satisfies ExportedHandler<EnvCore>;
