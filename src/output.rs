use std::io::{Stdout, Write};

use json::{JsonValue, object, Array as JsonArray};

use crate::{Error, Result};
use crate::eval::Row;

#[derive(Debug)]
pub enum Response {
    Ok,
    Error(Error),

    Done { affected_rows: usize },
    QueryHandleOpened { handle: u32, col_names: Vec<String> },
    QueryRow(Row),
    QueryRows {
        col_names: Option<Vec<String>>,
        stride: u32,
        values: Row,
    },
    AppendHandleOpened(u32),
}


pub fn write_response(stdout: &mut Stdout, resp: Response) -> Result<()> {
    let resp_json = match resp {
        Response::Ok => object!{
            "ok": true,
        },

        Response::Done { affected_rows } => object!{
            "ok": true,
            "aff": affected_rows,
        },

        Response::QueryHandleOpened { handle, col_names } => object!{
            "ok": true,
            "h": handle,
            "c": col_names,
        },

        Response::AppendHandleOpened(handle) => object!{
            "ok": true,
            "h": handle,
        },

        Response::QueryRow(values) => {
            let json_row: JsonArray = values
                .into_iter()
                .map(|val| val.into())
                .collect();
            object!{
                "ok": true,
                "r": json_row,
            }
        },

        Response::QueryRows { col_names, stride, values } => {
            let mut res = {
                let json_values = values
                    .into_iter()
                    .map(|val| -> JsonValue { val.into() })
                    .collect::<JsonArray>();
                let json_rows = json_values
                    .as_slice()
                    .chunks(stride as usize)
                    .collect::<Vec<_>>();

                object!{
                    "ok": true,
                    "rr": json_rows,
                }
            };

            if let Some(cols) = col_names {
                let cols_json: JsonValue = cols
                    .into_iter()
                    .map(|val| val.into())
                    .collect::<JsonArray>()
                    .into();
                res.insert("c", cols_json).unwrap();
            }

            res
        },

        Response::Error(err) => object!{
            "ok": false,
            "err": err.to_string(),
        },
    };

    let mut resp_str = json::stringify(resp_json);
    resp_str.push('\n');
    stdout.write_all(resp_str.as_bytes())?;
    stdout.flush()?;

    Ok(())
}
