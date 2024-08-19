use crate::io;
use std::fmt::{Debug, Display};
// use std::str::FromStr;
use json::{JsonValue, object::Object as JsonObject};

#[derive(Debug)]
pub enum Request {
    StartAppend(String),
    EndAppend,
    AppendRow(Vec<JsonValue>),
    RunSql(String, Option<Params>),
}

#[derive(Debug)]
pub enum Params {
    List(Vec<JsonValue>),
    Map(JsonObject),
}
impl TryFrom<&JsonValue> for Params {
    type Error = Error;

    fn try_from(value: &JsonValue) -> Result<Self> {
        match value {
            JsonValue::Array(arr) => Ok(Self::List(arr.clone())),
            JsonValue::Object(obj) => Ok(Self::Map(obj.clone())),
            _ => Err(Error::ParamsError("invalid params type")),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    UnknownMethod(String),
    ParamsError(&'static str),
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownMethod(method_name) => write!(f, "unknown method: {}", method_name),
            Self::ParamsError(msg) => write!(f, "invalid params: {}", msg),
        }
    }
}
impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub fn parse(req: &io::RpcRequest) -> Result<Request> {
    match req.method.as_str() {
        "as" => parse_start_append(&req.params),
        "a" => parse_append(&req.params),
        "ae" => Ok(Request::EndAppend),
        "sql" => parse_sql(&req.params),
        method => Err(Error::UnknownMethod(String::from(method))),
    }
}
fn parse_start_append(params: &JsonObject) -> Result<Request> {
    let table = params.get("t")
        .ok_or(Error::ParamsError("param `t` not specified"))
        .and_then(|v|
            v.as_str()
                .map(|str| String::from(str))
                .ok_or(Error::ParamsError("invalid type for param `t`")))?;

    Ok(Request::StartAppend(table))
}
fn parse_append(params: &JsonObject) -> Result<Request> {
    let row = params.get("r")
        .ok_or(Error::ParamsError("param `r` not specified"))?;

    match row {
        JsonValue::Array(vec) => Ok(Request::AppendRow(vec.clone())),
        _ => Err(Error::ParamsError("invalid type for param `r`")),
    }
}
fn parse_sql(params: &JsonObject) -> Result<Request> {
    let query = params.get("q")
        .and_then(|v| v.as_str())
        .map(|str| String::from(str))
        .ok_or(Error::ParamsError("invalid query type"))?;

    let params = params.get("p")
        .map(|val| Params::try_from(val))
        .transpose()?;

    Ok(Request::RunSql(query, params))
}
