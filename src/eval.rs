use std::pin::Pin;
use std::ptr::addr_of_mut;

use duckdb::{Connection, Statement, Rows, ToSql, types::ValueRef, params};
use json::JsonValue;

use crate::{Error, Result};
use crate::input::Query;

#[derive(Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl ToSql for Value {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        use duckdb::types::{Value as DdbValue, ToSqlOutput as Out};

        Ok(match self {
            Value::Null => Out::Owned(DdbValue::Null),
            Value::Bool(val) => Out::Owned(DdbValue::Boolean(*val)),
            Value::Int(val) => Out::Owned(DdbValue::BigInt(*val)),
            Value::Float(val) => Out::Owned(DdbValue::Double(*val)),
            Value::String(val) => Out::Borrowed(ValueRef::Text(val.as_bytes())),
        })
    }
}
impl From<Value> for JsonValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => JsonValue::Null,
            Value::Bool(val) => JsonValue::Boolean(val),
            Value::Int(val) => JsonValue::Number(val.into()),
            Value::Float(val) => JsonValue::Number(val.into()),
            Value::String(val) => JsonValue::String(val),
        }
    }
}

impl From<&JsonValue> for Value {
    fn from(value: &JsonValue) -> Self {
        match value {
            JsonValue::Null => Value::Null,
            JsonValue::Boolean(val) => Value::Bool(*val),
            JsonValue::Short(val) => Value::String(String::from(val.as_str())),
            JsonValue::String(val) => Value::String(val.clone()),
            JsonValue::Number(val) => {
                let v: f64 = (*val).into();
                if
                    v.trunc() != v
                    || v > (i64::MAX as f64)
                    || v < (i64::MIN as f64)
                {
                    Value::Float(v)
                } else {
                    Value::Int(v as i64)
                }
            },
            JsonValue::Object(_) => todo!(),
            JsonValue::Array(_) => todo!(),
        }
    }
}
impl<'a> From<ValueRef<'a>> for Value {
    fn from(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(val) => Value::Bool(val),
            ValueRef::TinyInt(val) => Value::Int(val.into()),
            ValueRef::SmallInt(val) => Value::Int(val.into()),
            ValueRef::Int(val) => Value::Int(val.into()),
            ValueRef::BigInt(val) => Value::Int(val),
            ValueRef::UTinyInt(val) => Value::Int(val as i64),
            ValueRef::USmallInt(val) => Value::Int(val as i64),
            ValueRef::UInt(val) => Value::Int(val as i64),
            ValueRef::UBigInt(val) => Value::Int(val as i64),
            ValueRef::Float(val) => Value::Float(val as f64),
            ValueRef::Double(val) => Value::Float(val),
            // TODO fallible conversion?
            ValueRef::Text(val) => Value::String(String::from(value.as_str().unwrap())),

            ValueRef::HugeInt(_) => todo!(),
            ValueRef::Decimal(val) => todo!(),
            ValueRef::Timestamp(_, _) => todo!(),
            ValueRef::Blob(_) => todo!(),
            ValueRef::Date32(_) => todo!(),
            ValueRef::Time64(_, _) => todo!(),
            ValueRef::Interval { months, days, nanos } => todo!(),
            ValueRef::List(_, _) => todo!(),
            ValueRef::Enum(_, _) => todo!(),
            ValueRef::Struct(_, _) => todo!(),
            ValueRef::Array(_, _) => todo!(),
            ValueRef::Map(_, _) => todo!(),
            ValueRef::Union(_, _) => todo!(),
        }
    }
}

pub type Row = Vec<Value>;

pub struct StmtHandle<'a> {
    pub stmt: Statement<'a>,
    pub rows: Option<Rows<'a>>,
}

impl<'a> StmtHandle<'a> {
    pub fn new(stmt: Statement<'a>) -> Self {
        StmtHandle { stmt, rows: None }
    }

    fn build_rows<F>(mut self: Pin<&mut Self>, f: F) -> duckdb::Result<()>
    where
        F: FnOnce(&'a mut Statement<'a>) -> duckdb::Result<Rows<'a>>,
    {
        // SAFETY: lifetime of `rows` will be the same as `stmt`, and it is
        // not allowed to move via `Pin`, so taking a pointer in a self-referential
        // fashion is okay.
        let ptr_ref = unsafe {
            let ptr = addr_of_mut!(self.stmt);
            ptr.as_mut().unwrap()
        };
        let rows = f(ptr_ref)?;
        self.rows = Some(rows);
        Ok(())
    }

    pub fn query(mut self: Pin<&mut Self>, query: &Query) -> Result<()> {
        self.build_rows(|stmt| {
            do_query(stmt, query, |s, p| s.query(p))
        })?;
        Ok(())
    }

    pub fn col_names(self: Pin<&Self>) -> Vec<String> {
        self.stmt.column_names()
    }

    fn fetch_row_impl(mut self: Pin<&mut Self>, cols: usize, into: &mut Row) -> Result<()> {
        match self.rows.as_mut().unwrap().next()? {
            None => Err(Error::Eof),
            Some(row) => {
                into.reserve(cols);
                for i in 0..cols {
                    let col = row.get_ref(i).unwrap();
                    into.push(col.into());
                }
                Ok(())
            }
        }
    }

    pub fn fetch_row(self: Pin<&mut Self>) -> Result<Row> {
        let cols = self.stmt.column_count();
        let mut row: Row = Vec::with_capacity(cols);
        self.fetch_row_impl(cols, &mut row)?;
        Ok(row)
    }

    pub fn fetch_rows(mut self: Pin<&mut Self>, rows: usize) -> Result<(usize, Row)> {
        let cols = self.stmt.column_count();
        let mut row_vec: Row = Vec::new();

        for _ in 0..rows {
            match self.as_mut().fetch_row_impl(cols, &mut row_vec) {
                Err(Error::Eof) => break,
                Err(err) => return Err(err),
                Ok(_) => (),
            }
        }

        if row_vec.is_empty() {
            return Err(Error::Eof);
        }

        Ok((cols, row_vec))
    }
}

fn do_query<'a, T, F>(stmt: &'a mut Statement<'a>, query: &Query, f: F) -> duckdb::Result<T>
where
    F: FnOnce(&'a mut Statement<'a>, &[&dyn ToSql]) -> duckdb::Result<T>
{
    match &query.params {
        Some(params) => {
            let witness: Vec<&dyn ToSql> = params
                .iter()
                .map(|x| x as &dyn ToSql)
                .collect();
            f(stmt, witness.as_slice())
        },
        None => f(stmt, params![])
    }
}

pub fn execute(db: &Connection, query: Query) -> Result<usize> {
    let mut stmt = db.prepare(&query.sql)?;
    let res = do_query(&mut stmt, &query, |s, p| s.execute(p))?;
    Ok(res)
}

pub fn prepare<'a>(db: &'a Connection, query: &'a Query) -> Result<StmtHandle<'a>> {
    let stmt = db.prepare(&query.sql)?;
    Ok(StmtHandle::new(stmt))
}
