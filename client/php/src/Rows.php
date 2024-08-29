<?php
declare(strict_types=1);
namespace PN\DuckRpc;

class Rows implements \IteratorAggregate
{
    private bool $isExhausted = false;

    public function __construct(
        private \Closure $call,
        private int $handle,
        private array $columns,
    ) {
    }

    public function __destruct()
    {
        ($this->call)('qx', [
            'h' => $this->handle,
        ]);
    }

    public function fetchChunk(int $size): ?array
    {
        if ($this->isExhausted) {
            return null;
        }

        $re = ($this->call)('qf', [
            'h' => $this->handle,
            'n' => $size,
        ]);
        $this->isExhausted = $re->eof;

        return array_map(function (array $row) {
            $obj = new \stdClass();
            foreach ($this->columns as $i => $col) {
                $obj->{$col} = $row[$i];
            }
            return $obj;
        }, $re->r);
    }

    public function fetch(): ?object
    {
        $row = $this->fetchChunk(1);
        if (!$row) {
            return null;
        }
        return $row[0];
    }

    public function getIterator(): \Traversable
    {
        while ($chunk = $this->fetchChunk(100)) {
            yield from $chunk;
        }
    }
}
