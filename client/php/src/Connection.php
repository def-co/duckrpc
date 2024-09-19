<?php
declare(strict_types=1);
namespace PN\DuckRpc;

class Connection
{
    private int $dbHandle;

    public function __construct(
        string $dbName,
        private Process $proc = new Process(),
    ) {
        $this->dbHandle = $this->proc->call('c', ['p' => $dbName])->d;
    }

    public function execute(string $query, array $params = []): void
    {
        $this->proc->call('e', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);
    }

    public function select(string $query, array $params = []): Rows
    {
        $re = $this->proc->call('q', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);

        return new Rows(
            $this->proc->call(...),
            $re->h,
            $re->c,
        );
    }

    public function selectOne(string $query, array $params = []): ?object
    {
        foreach ($this->select($query, $params) as $row) {
            return $row;
        }

        return null;
    }

    public function selectValue(string $query, array $params = []): mixed
    {
        $re = $this->proc->call('q', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);
        $handle = $re->h;

        try {
            $re = $this->proc->call('qf', [
                'h' => $handle,
                'n' => 1,
            ]);
            return $re->r[0][0];
        } finally {
            $this->proc->call('qx', [
                'h' => $handle,
            ]);
        }
    }

    public function selectAll(string $query, array $params = []): array
    {
        $re = $this->proc->call('qq', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);

        $rows = [];
        foreach ($re->r as $row) {
            $rowObj = new \stdClass();
            foreach ($re->c as $i => $col) {
                $rowObj->{$col} = $row[$i];
            }
            $rows[] = $rowObj;
        }

        return $rows;
    }

    public function appender(string $table): Appender
    {
        $re = $this->proc->call('a', [
            'd' => $this->dbHandle,
            't' => $table,
        ]);

        return new Appender(
            $this->proc->call(...),
            $re->h,
        );
    }
}
