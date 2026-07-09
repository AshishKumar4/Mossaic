export interface SqlMetrics {
  statements: number;
  reads: number;
  writes: number;
  other: number;
  rowsRead: number;
  rowsWritten: number;
}

function emptyMetrics(): SqlMetrics {
  return {
    statements: 0,
    reads: 0,
    writes: 0,
    other: 0,
    rowsRead: 0,
    rowsWritten: 0,
  };
}

function countedIterator<T>(
  iterator: IterableIterator<T>,
  capture: () => void
): IterableIterator<T> {
  return {
    next(): IteratorResult<T> {
      const result = iterator.next();
      capture();
      return result;
    },
    [Symbol.iterator](): IterableIterator<T> {
      return this;
    },
  };
}

export class CountingSqlStorage {
  private metrics = emptyMetrics();

  wrap(inner: SqlStorage): SqlStorage {
    const exec = <T extends Record<string, SqlStorageValue>>(
      query: string,
      ...bindings: unknown[]
    ): SqlStorageCursor<T> => {
      this.metrics.statements++;
      this.classify(query);

      const cursor = inner.exec<T>(query, ...bindings);
      let capturedRowsRead = 0;
      let capturedRowsWritten = 0;
      const capture = (): void => {
        const rowsRead = cursor.rowsRead;
        const rowsWritten = cursor.rowsWritten;
        this.metrics.rowsRead += rowsRead - capturedRowsRead;
        this.metrics.rowsWritten += rowsWritten - capturedRowsWritten;
        capturedRowsRead = rowsRead;
        capturedRowsWritten = rowsWritten;
      };

      capture();
      return new Proxy(cursor, {
        get(target, property) {
          if (property === "toArray") {
            return (): T[] => {
              try {
                return target.toArray();
              } finally {
                capture();
              }
            };
          }
          if (property === "one") {
            return (): T => {
              try {
                return target.one();
              } finally {
                capture();
              }
            };
          }
          if (property === "next") {
            return (): IteratorResult<T> => {
              try {
                const result = target.next();
                return result.done === true
                  ? { done: true, value: undefined }
                  : { done: false, value: result.value };
              } finally {
                capture();
              }
            };
          }
          if (property === "raw") {
            return <U extends SqlStorageValue[]>(): IterableIterator<U> =>
              countedIterator(target.raw<U>(), capture);
          }
          if (property === Symbol.iterator) {
            return (): IterableIterator<T> =>
              countedIterator(target[Symbol.iterator](), capture);
          }
          return Reflect.get(target, property, target);
        },
      });
    };

    return new Proxy(inner, {
      get(target, property) {
        if (property === "exec") return exec;
        return Reflect.get(target, property, target);
      },
    });
  }

  reset(): void {
    this.metrics = emptyMetrics();
  }

  snapshot(): SqlMetrics {
    return { ...this.metrics };
  }

  private classify(query: string): void {
    const operation = query.trimStart().match(/^[A-Za-z]+/)?.[0].toUpperCase();
    if (operation === "SELECT" || operation === "WITH") {
      this.metrics.reads++;
      return;
    }
    if (
      operation === "INSERT" ||
      operation === "UPDATE" ||
      operation === "DELETE" ||
      operation === "REPLACE"
    ) {
      this.metrics.writes++;
      return;
    }
    this.metrics.other++;
  }
}
