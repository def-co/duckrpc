use json::{self, JsonValue};
use std::error::Error as StdError;
use std::fmt::{Debug, Display};
use std::io::{self, Read};
use std::str::FromStr;

enum Parameter {
    String(String),
    Int(i64),
    Float(f64),
    Null,
    Bool(bool),
}
enum Command {
    Append(String),
    RunSql(String, Vec<Parameter>),
}

#[derive(Debug)]
enum RequestId {
    String(String),
    Short(json::short::Short),
    Number(json::number::Number),
}
impl TryFrom<json::JsonValue> for RequestId {
    type Error = Error;

    fn try_from(value: JsonValue) -> Result<Self> {
        match (value) {
            JsonValue::Short(short) => Ok(Self::String(String::from_str(short.as_str()).unwrap())),
            JsonValue::String(str) => Ok(Self::String(str)),
            JsonValue::Number(num) => Ok(Self::Number(num)),
            _ => Err(Error::Format("invalid request id type")),
        }
    }
}

#[derive(Debug)]
pub struct RpcRequest {
    pub id: RequestId,
    pub method: String,
    pub params: json::object::Object,
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Json(json::Error),
    Format(&'static str),
}
impl StdError for Error {}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self) {
            Self::Io(io) => std::fmt::Display::fmt(&io, f),
            Self::Json(json) => std::fmt::Display::fmt(&json, f),
            Self::Format(msg) => write!(f, "format error: {}", msg),
        }
    }
}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<json::Error> for Error {
    fn from(value: json::Error) -> Self {
        Self::Json(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn read_command(io: &io::Stdin) -> Result<RpcRequest> {
    let mut line = String::new();
    io.read_line(&mut line)?;

    let json = json::parse(line.as_str())?;
    let obj = match json {
        json::JsonValue::Object(obj) => obj,
        _ => return Err(Error::Format("request is not an object")),
    };

    let method = obj.get("@")
        .ok_or_else(|| Error::Format("method name key not present"))
        .and_then(|val| match val {
            JsonValue::Short(short) => Ok(String::from_str(short.as_str()).unwrap()),
            JsonValue::String(str) => Ok(str.clone()),
            _ => Err(Error::Format("invalid method name value")),
        })?;

    let request_id = obj.get("_")
        .ok_or_else(|| Error::Format("request id key not present"))
        .and_then(|val| RequestId::try_from(val.clone()))?;

    let mut params = obj.clone();
    params.remove("_");
    params.remove("@");

    Ok(RpcRequest{
        id: request_id,
        method,
        params,
    })
}
