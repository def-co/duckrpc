use std::cell::RefCell;
use std::rc::Rc;

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

type StatementCell<'a> = RefCell<Statement<'a>>;
type StatementRc<'a> = Rc<StatementCell<'a>>;
type RhRc<'a> = Rc<RefCell<RowsHandle<'a>>>;

struct RowsHandle<'a> {
    stmt_rc: Rc<RefCell<Statement<'a>>>,
    rows: Option<Rows<'a>>,
}
impl<'a> RowsHandle<'a> {
    // SAFETY: Skirts around ownership rules for Rc, so assumes that it will live long enough.
    unsafe fn build_rows<F: FnOnce(&'a mut Statement<'a>) -> Rows<'a>>(&mut self, f: F) {
        let ptr = self.stmt_rc.as_ptr();
        let ptr_ref = ptr.as_mut().unwrap();
        let rows = f(ptr_ref);
        self.rows = Some(rows);
    }
}

fn main() {
    let db = Connection::open_in_memory().unwrap();

    let mut stmt_handle: Option<StatementRc<'_>> = None;
    let mut rows_handle: Option<RhRc<'_>> = None;

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
                stmt_handle = Some(Rc::new(RefCell::new(stmt)));
            }
            Command::Query => {
                let stmt_rc = stmt_handle.as_ref().unwrap().clone();

                let mut rh = RowsHandle {
                    stmt_rc,
                    rows: None,
                };

                unsafe {
                    rh.build_rows(|stmt| stmt.query(params![]).unwrap());
                }

                rows_handle = Some(Rc::new(RefCell::new(rh)));
            }
            Command::Fetch1 => {
                let rh_rc = rows_handle.as_ref().unwrap().clone();
                let mut rh = rh_rc.borrow_mut();
                let rows = rh.rows.as_mut().unwrap();

                if let Some(row) = rows.next().unwrap() {
                    let id: i32 = row.get("id").unwrap();
                    let name: String = row.get("name").unwrap();
                    println!("got row: {}, {}", id, name);
                }
            },
            Command::Done => {
                rows_handle = None;
                stmt_handle = None;
            },
        }
    }
}
