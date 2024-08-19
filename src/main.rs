mod io;
mod eval;
mod map_value;

use thiserror::Error;
use duckdb::Connection;
use std::env::args;
use std::fmt::Debug;
use std::io::{stdin, stdout, Write};
use json::{object, array, JsonValue, stringify};

#[derive(Error, Debug)]
enum Error {
    #[error("io error")]
    Io(#[from] io::Error),

    #[error("eval error")]
    Eval(#[from] eval::Error),

    #[error("duckdb error")]
    DuckDb(#[from] duckdb::Error),

    #[error("stdio error")]
    StdIo(#[from] std::io::Error),

    #[error("not enough args, expected 2")]
    NotEnoughArgs,
}

fn connect() -> Result<Connection, Error> {
    let path = args()
        .nth(1)
        .ok_or(Error::NotEnoughArgs)?;

    Ok(Connection::open(path)?)
}

fn main() -> Result<(), Error> {
    let conn = connect()?;

    let stdin = stdin();
    let stdout = stdout();

    loop {
        let req = io::read_command(&stdin)?;
        println!("request: {:?}", req);
        let cmd = eval::parse(&req)?;
        println!("command: {:?}", cmd);
        eval_cmd(&conn, cmd)?
    }
}

fn eval_cmd(conn: &Connection, cmd: eval::Request) -> Result<(), Error> {
    match cmd {
        eval::Request::StartAppend(_) => todo!(),
        eval::Request::EndAppend => todo!(),
        eval::Request::AppendRow(_) => todo!(),
        eval::Request::RunSql(sql, params) => eval_sql(conn, sql, params),
    }
}

fn val_to_json(r: duckdb::types::ValueRef) -> JsonValue {
    use duckdb::types::ValueRef as VR;

    match r {
        VR::Null => JsonValue::Null,
        VR::Boolean(val) => JsonValue::Boolean(val),
        VR::TinyInt(val) => JsonValue::from(val),
        VR::SmallInt(val) => JsonValue::from(val),
        VR::Int(val) => JsonValue::from(val),
        VR::BigInt(val) => JsonValue::from(val),
        VR::HugeInt(_) => panic!("i128 conversion not implemented"),
        VR::UTinyInt(val) => JsonValue::from(val),
        VR::USmallInt(val) => JsonValue::from(val),
        VR::UInt(val) => JsonValue::from(val),
        VR::UBigInt(val) => JsonValue::from(val),
        VR::Float(val) => JsonValue::from(val),
        VR::Double(val) => JsonValue::from(val),
        VR::Decimal(_) => panic!("decimal conversion not implemented"),
        VR::Timestamp(_, _) => panic!("timestamp conversion not implemented"),
        VR::Text(val) => JsonValue::String(unsafe { String::from_utf8_unchecked(Vec::from(val)) }),
        VR::Blob(_) => todo!(),
        VR::Date32(_) => todo!(),
        VR::Time64(_, _) => todo!(),
        VR::Interval { months: _, days: _, nanos: _ } => todo!(),
        VR::List(_, _) => todo!(),
        VR::Enum(_, _) => todo!(),
        VR::Struct(_, _) => todo!(),
        VR::Array(_, _) => todo!(),
        VR::Map(_, _) => todo!(),
        VR::Union(_, _) => todo!(),
    }
}

fn eval_sql(conn: &Connection, sql: String, params: Option<eval::Params>) -> Result<(), Error> {
    let mut stmt = conn.prepare(sql.as_str())?;

    // let col_names = stmt.column_names();
    let mut res = if let Some(p) = params {
        todo!();
    } else {
        stmt.query([])?
    };
    // let col_names = stmt.column_names();

    let column_names = res.as_ref().unwrap().column_names();

    let mut rows = array![];
    while let Some(row) = res.next()? {
        let mut row_obj = object![];
        for (i, name) in column_names.iter().enumerate() {
            let val = row.get_ref(i)?;
            row_obj.insert(name.as_str(), val_to_json(val)).unwrap();
        }
        rows.push(row_obj);
    }

    let j = json::stringify(rows);
    stdout().write_fmt(format_args!("{}\n", j))?;
    Ok(())
}
