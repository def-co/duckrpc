use std::fmt::Debug;
use std::io::Stdin;
use json::{JsonValue, object::Object as JsonObject};

use crate::{Error, Result};
use crate::eval::Value;

#[derive(Debug)]
pub struct Query {
    pub sql: String,
    pub params: Option<Vec<Value>>,
}

#[derive(Debug)]
pub enum Command {
    Execute(Query),
    QueryImmediate(Query),
    QueryHandle(Query),
    QueryRow,
    QueryRows(u32),
    AppendPrepare(Query),
    AppendRow(u32, Vec<Value>),
    AppendRows(u32, Vec<Value>),
    AppendFlush,
}

pub fn read_command(stdin: &mut Stdin) -> Result<Command> {
    let mut row = String::new();
    if stdin.read_line(&mut row)? == 0 {
        return Err(Error::Eof);
    }

    let obj = match json::parse(&row)? {
        JsonValue::Object(obj) => obj,
        _ => return Err(Error::General("request not object")),
    };

    let meth = match obj.get("@")
        .ok_or(Error::General("method key absent"))?
    {
        JsonValue::Short(str) => str.to_string(),
        JsonValue::String(str) => str.to_string(),
        _ => return Err(Error::General("method not string")),
    };

    let command = match meth.as_str() {
        "e" => Command::Execute(read_query(&obj)?),
        "q" => Command::QueryImmediate(read_query(&obj)?),
        "qh" | "qr" | "qrr" | "a" | "ar" | "arr" | "ae" => todo!(),
        _ => return Err(Error::General("method unknown")),
    };
    Ok(command)
}

fn read_query(obj: &JsonObject) -> Result<Query> {
    let query = obj.get("q")
        .ok_or(Error::General("query key absent"))?
        .as_str()
        .ok_or(Error::General("query not string"))?;

    let params = if let Some(params) = obj.get("p") {
        let params_arr = match params {
            JsonValue::Array(arr) => arr,
            _ => return Err(Error::General("params not array")),
        };

        let p = params_arr.iter()
            // TODO fallible
            .map(|val| -> Value { val.into() })
            .collect::<Vec<_>>();

        Some(p)
    } else {
        None
    };

    Ok(Query {
        sql: String::from(query),
        params,
    })
}
