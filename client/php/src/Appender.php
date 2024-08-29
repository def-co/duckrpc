<?php declare(strict_types=1);
namespace PN\DuckRpc;

class Appender
{
    private bool $isCommitted = false;

    public function __construct(
        private \Closure $call,
        private int $handle,
    ) {
    }

    public function __destruct()
    {
        if ($this->isCommitted) {
            return;
        }

        ($this->call)('ax', [
            'h' => $this->handle,
        ]);
    }

    public function insertRow(array $row): void
    {
        $this->insertRows([$row]);
    }

    public function insertRows(array $rows): void
    {
        ($this->call)('ai', [
            'h' => $this->handle,
            'r' => $rows,
        ]);
    }

    public function commit(): void
    {
        ($this->call)('ax', [
            'h' => $this->handle,
        ]);
        $this->isCommitted = true;
    }
}
