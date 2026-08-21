import type { EnvCore } from "@shared/types";
import { ShardDO } from "@core/objects/shard/shard-do";
import { UserDOCore } from "@core/objects/user/user-do-core";
import { CountingSqlStorage, type SqlMetrics } from "./counting-sql-storage";

export interface BenchmarkMetrics {
  sql: SqlMetrics;
  rpcCalls: number;
}

export class BenchmarkUserDO extends UserDOCore {
  private sqlCounter: CountingSqlStorage | undefined;
  private rpcCalls = 0;

  async benchmarkResetSqlMetrics(): Promise<void> {
    if (this.sqlCounter === undefined) {
      this.sqlCounter = new CountingSqlStorage();
      this.sql = this.sqlCounter.wrap(this.sql);
    }
    this.sqlCounter.reset();
    this.rpcCalls = 0;
  }

  async benchmarkMetrics(): Promise<BenchmarkMetrics> {
    if (this.sqlCounter === undefined) {
      throw new Error("SQL metrics were not started");
    }
    return { sql: this.sqlCounter.snapshot(), rpcCalls: this.rpcCalls };
  }

  protected override recordRpc(): void {
    this.rpcCalls++;
  }

  benchmarkMaintainVersionRetentionOrder(): boolean {
    this.rpcCalls++;
    return this.maintainVersionRetentionOrder();
  }
}

export class BenchmarkShardDO extends ShardDO {
  private sqlCounter: CountingSqlStorage | undefined;
  private rpcCalls = 0;

  async benchmarkResetSqlMetrics(): Promise<void> {
    if (this.sqlCounter === undefined) {
      this.sqlCounter = new CountingSqlStorage();
      this.sql = this.sqlCounter.wrap(this.sql);
    }
    this.sqlCounter.reset();
    this.rpcCalls = 0;
  }

  async benchmarkMetrics(): Promise<BenchmarkMetrics> {
    if (this.sqlCounter === undefined) {
      throw new Error("SQL metrics were not started");
    }
    return { sql: this.sqlCounter.snapshot(), rpcCalls: this.rpcCalls };
  }

  protected override recordRpc(): void {
    this.rpcCalls++;
  }

  benchmarkRunAlarm(): Promise<void> {
    return this.alarm();
  }
}

export default {
  fetch(): Response {
    return new Response("Mossaic storage benchmark");
  },
} satisfies ExportedHandler<EnvCore>;
