<?php declare(strict_types=1);
namespace PN\DuckRpc;

class Process
{
    public static string $executablePath;

    private $proc;
    private $stdin;
    private $stdout;
    private $stderr;

    public function __construct()
    {
        $this->proc = proc_open(
            command: [self::$executablePath],
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

        [$this->stdin, $this->stdout, $this->stderr] = $pipes;

        $res = $this->read();
        if ( ! $res->ok) {
            array_map(fclose(...), $pipes);
            throw new \RuntimeException("failed to boot: {$res->err}");
        }
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

    public function call(string $method, array $args): object
    {
        $this->write($method, $args);
        $re = $this->read();
        if ( ! $re->ok) {
            throw new \RuntimeException($re->err);
        }
        return $re;
    }
}
