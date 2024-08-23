use std::pin::Pin;
use std::ptr::addr_of_mut;

use duckdb::{params, Connection, Rows, Statement};

enum Command {
    Prepare,
    Query,
    Fetch1,
    Done,
}

const COMMANDS: &[Command] = &[
    Command::Prepare,
    Command::Query,
    Command::Fetch1,
    Command::Fetch1,
    Command::Fetch1,
    Command::Done,
];

struct StmtHandle<'a> {
    stmt: Statement<'a>,
    rows: Option<Rows<'a>>,
}

impl<'a> StmtHandle<'a> {
    fn new(stmt: Statement<'a>) -> Self {
        StmtHandle { stmt, rows: None }
    }

    fn build_rows<F>(mut self: Pin<&mut Self>, f: F)
    where
        F: FnOnce(&'a mut Statement<'a>) -> Rows<'a>,
    {
        // SAFETY: lifetime of `rows` will be the same as `stmt`, and it is
        // not allowed to move via `Pin`, so taking a pointer in a self-referential
        // fashion is okay.
        let ptr_ref = unsafe {
            let ptr = addr_of_mut!(self.stmt);
            ptr.as_mut().unwrap()
        };
        let rows = f(ptr_ref);
        self.rows = Some(rows);
    }
}

fn main() {
    let db = Connection::open_in_memory().unwrap();

    let mut stmt_handle: Option<Pin<Box<StmtHandle<'_>>>> = None;

    db.execute(
        "
        create table items (id integer primary key, name varchar)
        ",
        params![],
    )
    .unwrap();

    {
        let mut app = db.appender("items").unwrap();
        app.append_rows([params![1, "one"], params![2, "two"], params![3, "three"]])
            .unwrap();
        app.flush().unwrap();
    }

    for command in COMMANDS {
        match command {
            Command::Prepare => {
                let stmt = db.prepare("select * from items").unwrap();
                let sh = StmtHandle::new(stmt);
                stmt_handle = Some(Box::pin(sh));
            }
            Command::Query => {
                let sh = stmt_handle.as_mut().unwrap();
                sh.as_mut()
                    .build_rows(|stmt| stmt.query(params![]).unwrap());
            }
            Command::Fetch1 => {
                let sh = stmt_handle.as_mut().unwrap();

                if let Some(row) = sh.rows.as_mut().unwrap().next().unwrap() {
                    let id: i32 = row.get("id").unwrap();
                    let name: String = row.get("name").unwrap();
                    println!("got row: {}, {}", id, name);
                }
            }
            Command::Done => {
                // rows_handle = None;
                stmt_handle = None;
            }
        }
    }
}
