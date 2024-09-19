<?php
declare(strict_types=1);
namespace PN\DuckRpc;

class Connection
{
    public static string $executablePath;

    private int $dbHandle;
    private $proc;
    private $stdin;
    private $stdout;
    private $stderr;

    public function __construct(
        string $dbName,
    ) {
        $this->proc = proc_open(
            command: [
                self::$executablePath,
                $dbName,
            ],
            descriptor_spec: [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            pipes: $pipes,
        );

        if (!$this->proc) {
            throw new \RuntimeException('failed to proc_open');
        }

        $this->stdin = $pipes[0];
        $this->stdout = $pipes[1];
        $this->stderr = $pipes[2];

        $res = $this->read();
        if (!$res->ok) {
            array_map(fclose(...), $pipes);
            throw new \RuntimeException("failed to boot: {$res->err}");
        }

        $res = $this->call('c', ['p' => $dbName]);
        if (!$res->ok) {
            throw new \RuntimeException("failed to open db: {$res->err}");
        }
        $this->dbHandle = $res->d;
    }

    public function __destruct()
    {
        $this->call('x', []);
        array_map(fclose(...), [$this->stdin, $this->stdout, $this->stderr]);
        proc_close($this->proc);
    }

    private function read(): object
    {
        $line = fgets($this->stdout);
        return json_decode($line, flags: JSON_THROW_ON_ERROR);
    }

    private function write(string $method, array $args): void
    {
        $req = new \stdClass();
        $req->{'@'} = $method;
        foreach ($args as $key => $value) {
            $req->{$key} = $value;
        }

        $line = json_encode(
            $req,
            flags: JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES,
        ) . "\n";
        fwrite($this->stdin, $line);
    }

    private function call(string $method, array $args): object
    {
        $this->write($method, $args);
        $re = $this->read();
        if (!$re->ok) {
            throw new \RuntimeException($re->err);
        }
        return $re;
    }

    public function execute(string $query, array $params = []): void
    {
        $this->call('e', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);
    }

    public function select(string $query, array $params = []): Rows
    {
        $re = $this->call('q', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);

        return new Rows(
            $this->call(...),
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
        $re = $this->call('q', [
            'd' => $this->dbHandle,
            'q' => $query,
            'p' => $params,
        ]);
        $handle = $re->h;

        try {
            $re = $this->call('qf', [
                'h' => $handle,
                'n' => 1,
            ]);
            return $re->r[0][0];
        } finally {
            $this->call('qx', [
                'h' => $handle,
            ]);
        }
    }

    public function selectAll(string $query, array $params = []): array
    {
        $re = $this->call('qq', [
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
        $re = $this->call('a', [
            'd' => $this->dbHandle,
            't' => $table,
        ]);

        return new Appender(
            $this->call(...),
            $re->h,
        );
    }
}
